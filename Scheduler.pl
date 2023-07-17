#Imports
use IDAS;                     # For CSV2List()
use Logger;                   # For Logging Start Up
use POSIX qw(WNOHANG);        # For waitpid() so the aren't any hanging
use Data::Dumper qw(Dumper);  # For Debugging


my $logger  = Logger->new();  # A logging package

# Check to see if there are any other Schedulers running

my $ps = `ps -ef | grep Scheduler`;
my @ps = split("\n", $ps);
foreach (@ps) {
   my @temp = split('\s+', $_);
   if ($temp[2] ne $$ and $temp[1] eq $$){
   }elsif($temp[2] eq $$){
   }elsif($temp[7] eq "grep" or $temp[8] eq "grep"){
   }
   else{
      $logger->logF("There is already an NNR_Scheduler Running, closing this instance with pid $$");
      exit(1);
   }
}

# Open the file for reading
open my $fh, '<', 'test.csv' or die "Cannot open file: $!";
# Read the entire file into a string
my $csv;
{
    local $/;
    $csv = <$fh>;
}
# Close the file
close $fh;


@scripts = csv2list($csv); # Custom library to turn csvs into lists
%scripts = ();


my %completed;
my %incomplete;
for my $temp (@scripts) {
   my @temp = @$temp;
   my @deps = csv2list($temp[3]) if $temp[3];
   if (-e $temp[1]) {
      $scripts{$temp[0]} = {
         name           => $temp[0],
         loc            => $temp[1],
         time           => $temp[2],
         dependencies   => \@deps || ""
      };
      if($temp[3]){
         for my $dep (@{$scripts{$temp[0]}{dependencies}}){
            $incomplete{$temp[0]}{$dep} = 1;
         }
      }else{
         $incomplete{$temp[0]} = 1;
      }

      $logger->logF($temp[0]." has been added to execution list");
   }else{
      $logger->logF("This file doesn't exist: ".$temp[0]);
   }
}

my %children;


while(keys %incomplete){

   # Check to see if there are any scripts that have finished
   $logger->logF("Checking to see if any Scripts have completed");
   foreach (keys %incomplete) {
      my $pid = waitpid(-1, WNOHANG);
      if ($pid){
         if ($children{$pid}){
            $logger->logF($children{$pid}." has finished");
            $complete{$children{$pid}} = 1;
            delete $children{$pid};
         }
      }
   }

   for my $script (keys %incomplete) {
      # Run any scripts that have no remaining dependancies
      my $remaining_deps = 0;
      my $total_deps = 0;
      $logger->logF("Checking dependencies for $script");
      for my $dep (keys %{$incomplete{$script}}){
         $total_deps++;
         $remaining_deps++;
         if ($complete{$dep}) {
            $remaining_deps--;
            delete($incomplete{$script}{$dep});
         }
      }
      if ($remaining_deps == 0) {
         delete($incomplete{$script});
         $logger->logF("Starting $script");
         my $pid = fork();
         if ($pid) {
            $children{$pid} = $script;
         }
         else {
            my $run = $scripts{$script}->{loc};
            my $error = `$run`;
            $logger->logF("$script has finished running");
            exit(0);
         }
      }
   }
   # Chill for a second
   sleep(3);
}
1;
