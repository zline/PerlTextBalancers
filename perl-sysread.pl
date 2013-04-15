#!/usr/bin/perl

# TODO TODO TODO глобальный лок балансировки
# TODO TODO TODO параметр числа процессов
# TODO TODO TODO поддержка передачи объектов, callback'и до и после обработки

# NOTE не бьет строки в ходе обработки, но порядок результирующих данных не гарантирован

use strict;
use warnings;

use Fcntl qw(:flock);
use File::Temp qw/ :mktemp  /;


main();


sub balance
{
    my ($rcRoutine, $fhIN, $fhOUT, %opts) = @_;
    
    _outputWrite($fhOUT, []);   # flush output
    
    my ($fh, $sInputLockFile, $sOutputLockFile);
    ($fh, $sInputLockFile) = mkstemp("/tmp/perl-balance.lock.$$.XXXXX");
    close($fh);
    ($fh, $sOutputLockFile) = mkstemp("/tmp/perl-balance.lock.$$.XXXXX");
    close($fh);
    
    my @aChildPids;
    for (1..3)
    {
        defined(my $pid = fork()) || die("fork() failed: $!");
        if (0 == $pid)
        {
            _balanceWorker($rcRoutine, $fhIN, $fhOUT, $sInputLockFile, $sOutputLockFile, %opts);
            exit(0);
        }
        else
        {
            push(@aChildPids, $pid);
        }
    }
    
    for my $iPid (@aChildPids)
    {
        waitpid($iPid, 0) > 0 || die("strange waitpid() return value for pid $iPid");
        0 == $? || die("child $iPid returns $?");
    }
    
    unlink($sInputLockFile, $sOutputLockFile);
}

sub _balanceWorker
{
    my ($rcRoutine, $fhIN, $fhOUT, $sInputLockFile, $sOutputLockFile, %opts) = @_;
    
    my $iProcessBlockSize = 10*1024*1024;
    my ($fhInputLock, $fhOutputLock);
    open($fhInputLock, '>'.$sInputLockFile) || die("child $$: failed to open $sInputLockFile: $!");
    open($fhOutputLock, '>'.$sOutputLockFile) || die("child $$: failed to open $sOutputLockFile: $!");
    
    my @aPortion;
    my @aData;
    my $bGotData = 1;
    while ($bGotData)
    {
        @aData = ();
        my $iBytesRead = 0;
        my $str;
        my $iRC;
        # читаем порцию данных
        flock($fhInputLock, LOCK_EX) or die();
        while ($bGotData && $iBytesRead < $iProcessBlockSize)
        {
            $iRC = sysread($fhIN, $str, 65536);
            defined($iRC) || die("child $$: sysread failed: $!");
            if (0 == $iRC)
            {
                $bGotData = 0;
                last;
            }
            
            push(@aData, $str);
            $iBytesRead += $iRC;
        }
        # дочитываем строковую запись, избегая буферизации
        while ($bGotData && $str ne "\n")
        {
            $iRC = sysread($fhIN, $str, 1);
            defined($iRC) || die("child $$: sysread failed: $!");
            if (0 == $iRC)
            {
                $bGotData = 0;
                last;
            }
            push(@aData, $str);
        }
        flock($fhInputLock, LOCK_UN) or die();
    
        last if (! @aData);
        @aPortion = split(/^/m, join('', @aData));
        
        my $raResult = $rcRoutine->(\@aPortion);
        
        # запись порции данных
        flock($fhOutputLock, LOCK_EX) || die("cannot lock output: $!");
        _outputWrite($fhOUT, $raResult);
        flock($fhOutputLock, LOCK_UN) || die("cannot unlock output: $!");
    }
    
    close($fhInputLock) || die("child $$: failed to close $sInputLockFile: $!");
    close($fhOutputLock) || die("child $$: failed to close $sOutputLockFile: $!");
}

sub _outputWrite
{
    my ($fh, $raData) = @_;
    
    my $fhOldSelected = select($fh);
    my $iOldAutoflushMode = $|;
    
    $| = 0; # буферизуем
    print @$raData;
    $| = 1; # flush - без него данные в выходном потоке будут смешаны на границах обработанных блоков
    
    # восстанавливаем исходное состояние
    $| = $iOldAutoflushMode;
    select($fhOldSelected);
}


sub main
{
#     print STDERR "pid $$\n";
#     sleep 10;
    
    balance(sub
    {
        my ($raPortion) = @_;
        
#         foreach my $line (@$raPortion)
#         {
#             my %h = map { $_ => 1 } split(/\s+/, lc($line));
#             $line = join(' ', sort keys(%h)) . "\n";
#         }
#         
        return $raPortion;
    }
    , \*STDIN, \*STDOUT);
}
