# CLI and Admin Client #

We provide a command line admin client to perform administrative
actions, like reporting current status of all CPFS servers, showing
disk space consumption of each DS, changing the log level, or shutting
down the whole cluster.  The admin client connects to the CPFS MS to
manage the CPFS cluester.

The message protocol of an admin client is very similar to a normal
filesystem client, but it allows a different set of FIMs.  The admin
client does not connect to the the DSs.  Disk usage information is
collected through the active MS.  Other changes affecting the DSs,
e.g., log level changes to apply to the DSs or shutdown requests, are
also routed through the active MS.

Like filesystem clients connection requests, admin client requests are
rejected if the MS is still in a starting state.  But there is a
catch: the connection FIM may have a "force start" flag set to cause
the MS to try starting service forcefully.  This would succeed even if
the standby MS is not connected yet, or some DSG only has 4 DSs
instead of 5.  So even though the connection is still rejected, it has
already affected the state of the system.

Admin clients will not participate in the failover procedures, and
accordingly the system will not wait for an admin client to reconnect
before becoming online again.  Because the CLI is typically
short-lived process that can be rerun if failed, it is considered not
essential to have the reliability of the filesystem clients.
