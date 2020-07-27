# Prosper Miner

The prosper-miner binary is a PegNet miner compatible with the Prosper pool server. When a miner submits their work to the pool in the form of a share, the pool software determines whether or not to submit that share, and will credit the miner for the work they have done.

## Performance Notes

The pool must be compiled with GoLang 1.13+, however the miner does not. There is some significant performance changes both positive and negative with GoLang 1.13 compliation of the miner for some platforms. It would be advisable to do some testing and figure out the best compliation options to maximize your hashrate. 

# Initial Setup

Initital setup of a miner requires some additional parameters to register your user account. A single user can have many miners, where a miner is a single instance of `prosper-miner`. A user receives all credit for a miner's work, and all payouts resulting from the miner's work. 

To run your first miner against the pool, an invite code must be provided. The current pool is setup as invite only. The params:

- Username: This is an email address that will serve as your login. Provide a legit one please, as it must be valid.
- Password: A prompt will come up to type in a password. The password is not needed for future miners, but it will be needed to login through any web portal
- MinerID: This is optional. You can provide a custom minerid that can be used to identify a specific miner. If you leave this blank, one will be chosen for you.
- PayoutAddress: A factoid address that will associated with your account for payouts
- Invite Code: This is needed for registration, ask the pool operator for one.

```
#		   pool-address          username            invitecode  pass    PayoutAddress
./prosper-miner -s 123.45.67.89:1234 -u user@example.com -i invite-EAXPRO -p -a FA2jK2HcLnRdS94dEcU27rF3meoJfpUcZPSinpb7AwQvPRY6RL1Q

```

The configuration for the pool miner is stored at `~/.prosper/prosper-miner.toml`. This path can be changed with the `--config`.

# Running Further Miners

Once you have an account, all further miners only need the username and optional minerid. If no minerid is provided, a random one is chosen for you. The miner will report stats at the completion of every job. The miner will begin mining as soon as the pool tells it too (within seconds of a successful connection). The miner will start automatically with the number of CPU's found, you can change this with `-t`. E.g `-t 10` will use 10 threads.

```
./prosper-miner --poolhost 123.45.67.89:1234 --user user@example.com -m machine01
```




# Command-line options

You can use `prosper-miner --help` to list the command-line arguments and options:

```
Usage:
  prosper-miner [flags]

Flags:
  -c, --config string       config path location
  -h, --help                help for prosper-miner
  -i, --invitecode string   Invite code for initial user registration
  -m, --minerid string      Minerid should be unique per mining machine (default is randomly generated)
  -t, --miners int          Number of mining threads (default 8)
  -p, --password            Enable password prompt for user registration
  -s, --poolhost string     URL to connect to the pool (default "localhost:1234")
  -u, --user string         Username to log into the mining pool

```

# Environment variables

The `prosper-miner` program can use the following environment variables:

<dl>
  <dt><var>PROSPERPOOL_CONFIG</var></dt>
  <dd>When the <var>PROSPERPOOL_CONFIG</var> variable is set, <code>prosper-miner</code> will use the configuration file identified <var>PROSPERPOOL_CONFIG</var> unless the <code>--config</code> argument is passed.</dd>
</dl>
