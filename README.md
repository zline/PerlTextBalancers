PerlTextBalancers
=================

Experiment with ways of efficient parallel processing of a text stream in perl.

The goal is to replace
   zcat ... | sort .. | .. bla-bla .. | cpu-intensive-slow-code.pl | .. bla-bla > out
with
   zcat ... | sort .. | .. bla-bla .. | cpu-intensive-but-parallel-so-N-times-faster-code.pl | .. bla-bla > out
(of course Im talking about wallclock time speedup, not cpu time)
by wrapping payload of cpu-intensive-slow-code.pl with parallelization wrapper with small overhead.

perl-sysread.pl works
and perl-piped.pl is broken
