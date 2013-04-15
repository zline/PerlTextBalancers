#!/usr/bin/perl

# FIXME FIXME FIXME FIXME буферизация ломает входные данные

# FIXME FIXME FIXME проблема с блокировками
# FIXME FIXME TODO вариант с перезаписью массива @aData
# FIXME FIXME TODO вариант с read()'ом в главном процессе
# FIXME FIXME TODO вариант с File::Copy в главном процессе
# FIXME FIXME TODO вариант без пайпа и с sysread'ами в потомках

use strict;
use warnings;

use Fcntl qw(:flock);
use File::Copy;


main();


sub balance
{
    my ($rcRoutine, $fhIN, $fhOUT, %opts) = @_;
    
    my ($fhPipeReader, $fhPipeWriter);
    pipe($fhPipeReader, $fhPipeWriter) || die("pipe() failed: $!");
    
    my @aChildPids;
    for (1..3)
    {
        defined(my $pid = fork()) || die("fork() failed: $!");
        if (0 == $pid)
        {
            close($fhPipeWriter) || die("failed to close() pipe writer in child process: $!");
            _balanceWorker($rcRoutine, $fhPipeReader, $fhOUT, %opts);
            
            exit(0);
        }
        else
        {
            push(@aChildPids, $pid);
        }
    }
    
    local $SIG{PIPE} = sub { die('failed to write to pipe') };
    
    copy($fhIN, $fhPipeWriter, 4096) || die("copy() failed: $!");
#     my $iCount = 0;
#     my $str;
#     while ()
#     {
#         $str = <$fhIN>;
#         last if (! defined($str));
#         
#         print $fhPipeWriter $str;
#         $iCount++;
#     }
    close($fhPipeWriter) || die("failed to close() pipe writer: $!");
    
    for my $iPid (@aChildPids)
    {
        waitpid($iPid, 0) > 0 || die("strange waitpid() return value for pid $iPid");
        0 == $? || die("child $iPid returns $?");
    }
    close($fhPipeReader) || die("failed to close() pipe reader: $!");
}

sub _balanceWorker
{
    my ($rcRoutine, $fhIN, $fhOUT, %opts) = @_;
    
    my @aData;
    my $bGotData = 1;
    while ($bGotData)
    {
        # читаем порцию данных
        my $str;
        @aData = ();
        flock($fhIN, LOCK_EX) || die("cannot lock pipe: $!");
        for (my $i = 0; $i < 50000; $i++)
        {
            $str = <$fhIN>;
            if (! defined($str))
            {
                $bGotData = 0;
                last;
            }
            else
            {
                push(@aData, $str);
            }
        }
        flock($fhIN, LOCK_UN) || die("cannot unlock pipe: $!");
        last if (! @aData);
        
        my $raResult = $rcRoutine->(\@aData);
        
        # запись порции данных
        flock($fhOUT, LOCK_EX) || die("cannot lock output: $!");
        print $fhOUT @$raResult;
        flock($fhOUT, LOCK_UN) || die("cannot unlock output: $!");
    }
}


sub main
{
    print "parent $$\n";
    sleep 20;
    
    balance(sub
    {
        my ($raPortion) = @_;
        return $raPortion;
    }
    , \*STDIN, \*STDOUT);
}
