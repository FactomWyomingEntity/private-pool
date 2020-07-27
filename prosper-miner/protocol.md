# Prosper Pool Miner Socket Protocol

The Prosper Pool miner opens a socket when run.  This socket provides methods for control of the miner and notifications about the status of the miner and mining.

The miner runs a JSON-RPC 2.0 server on the socket.  All methods use the "mining" namespace.

This document describes the Prosper Pool Miner Socket Protocol, version 0.1.

## Methods

### getStatus

The `getStatus` method reports the current status of the miner.  The method requires no arguments and returns a JSON object with the following keys:

<dl>
  <dt>isRunning</dt>
  <dd>_bool_: always present.  This value indicates whether the miner is configured to mine if possible.</dd>
  <dt>isConnected</dt>
  <dd>_bool_: always present.  This value indicates whether the miner is connected to the pool server.</dd>
  <dt>poolHostAndPort</dt>
  <dd>_string_: present when the miner is connected to the pool server.  This value indicates the IP address and the port of the remote server, separated by a colon.  IPv6 addresses are enclosed in square brackets.</dd>
  <dt>durationConnected</dt>
  <dd>_uint64_: present when the miner is connected to the pool server.  This value indicates seconds since the connection was last established.  This value resets to zero when a new connection is established.</dd>
  <dt>blocksSubmitted</dt>
  <dd>_uint64_: always present.  This value is a count of shares submitted to the pool server since the miner started running.  It does not reset to zero when a new connection is established.  This value resets to zero when the miner process terminates.</dd>
  <dt>statusCode</dt>
  <dd>_uint64_: always present.  This value indicates the miner's state in greater detail.</dd>
</dl>

Possible `statusCode` values include:

<dl>
  <dt>101</dt>
  <dd>The miner is starting up.</dd>
  <dt>102</dt>
  <dd>The miner is idle.</dd>
  <dt>103</dt>
  <dd>The miner is generating the LXRHash table file.</dd>
  <dt>104</dt>
  <dd>The miner is attempting to connect to the pool server.</dd>
  <dt>105</dt>
  <dd>The miner is connected and mining.</dd>
  <dt>-101</dt>
  <dd>The miner is cannot find a configuration file.</dd>
  <dt>-102</dt>
  <dd>The miner is unable to read the configuration file.</dd>
  <dt>-103</dt>
  <dd>The miner configuration is invalid.</dd>
  <dt>-104</dt>
  <dd>The miner is unable to write the configuration file.</dd>
  <dt>-105</dt>
  <dd>The miner is unable to read the LXRHash table file.</dd>
  <dt>-106</dt>
  <dd>The miner is unable to write the LXRHash table file.</dd>
</dl>

#### Examples

This example shows the response from a miner that has run for a short time and submitted a single share to the pool server.  The miner was then stopped.

```
>> {"jsonrpc": "2.0", "id": 4, "method": "mining_getStatus"}
<< {"jsonrpc": "2.0", "id": 4, "result": {"isRunning": false, "isConnected": false, "blocksSubmitted": 1}}
```

This example shows the response from a miner that has been running for a short time and is currently connected to the pool server.

```
>> {"jsonrpc": "2.0", "id": 6, "method": "mining_getStatus"}
<< {"jsonrpc": "2.0", "id": 6, "result": {"isRunning": true, "isConnected": true, "poolHostAndPort": "3.233.176.186:1234", "durationConnected": 73, "blocksSubmitted": 4}}
```

### register

The `register` method registers a new account with the mining pool.  The method requires several arguments:

<dl>
  <dt>emailAddress</dt>
  <dd>_string_: the e-mail address for the account, which functions as a username</dd>
  <dt>minerId</dt>
  <dd>_string_: an identifier for the miner which is running the command.  This is used to identify which miner belonging to an account has submitted which shares.</dd>
  <dt>password</dt>
  <dd>_string_: the password for this account, used to access the account interface at https://prosperpool.io/</dd>
  <dt>inviteCode</dt>
  <dd>_string_: an invitation code, required to register during the beta period.  See the Getting Started Guide at https://prosperpool.io for instructions on obtaining an invitation code.</dd>
  <dt>payoutAddress</dt>
  <dd>_string_: a Factoid address used for payouts.  This address should be one which the user controls.</dd>
</dl>

The `register` method parameters are passed to `func NewClient(…)` in `../stratum/client.go`.

The `register` method returns a _bool_ indicating success.  Failure returns a [JSON-RPC error][json-rpc error specification] object with one of the following codes:

<dl>
  <dt>-301</dt>
  <dd>The e-mail address is invalid.</dd>
  <dt>-302</dt>
  <dd>The password is invalid.</dd>
  <dt>-303</dt>
  <dd>The invite code is invalid.</dd>
  <dt>-304</dt>
  <dd>The payout address is invalid.</dd>
</dl>

#### Examples

Do not use the e-mail addresses, passwords, invite codes or Factoid addresses shown in these examples.  You will be sorry if you do.

This example shows successful registration.

```
>> {"jsonrpc": "2.0", "id": 1, "method": "mining_register", params: ["user@example.com", "desktop", "sweetly_sings-my33rose", "49uE1bxYmtZjLemZhTeXOuUTh8Ee", "FA38eZQPdMN3oRQ6b1QG14kbQGVEkkaGEhtZDyQtWUDoydjfjvTU"]}
<< {"jsonrpc": "2.0", "id": 1, "result": {"success": true}}
```

This example shows failed registration because the e-mail address is invalid.

```
>> {"jsonrpc": "2.0", "id": 1, "method": "mining_register", params: ["user2example.com", "desktop", "!noisy9392FLOWERS!", "49uE1bxYmtZjLemZhTeXOuUTh8Ee", "FA38eZQPdMN3oRQ6b1QG14kbQGVEkkaGEhtZDyQtWUDoydjfjvTU"]}
<< {"jsonrpc": "2.0", "id": 1, "result": {"success": false, "errorMessage": "Please correct the e-mail address.", "validationError": {"emailAddress": "The e-mail address does not contain an at sign (@)."}}
```

This example shows failed registration because the invite code is empty.

```
>> {"jsonrpc": "2.0", "id": 1, "method": "mining_register", params: ["user@example.com", "desktop", "tehD415y_protests", "", "FA38eZQPdMN3oRQ6b1QG14kbQGVEkkaGEhtZDyQtWUDoydjfjvTU"]}
<< {"jsonrpc": "2.0", "id": 1, "result": {"success": false, "errorMessage": "Please provide an invite code.", "validationError": {"inviteCode": "The invite code is empty."}}
```

### start

The `start` method requests that the miner begin mining.  The method requires no arguments and returns a _bool_, `true`, to indicate that the request has been received.

#### Example

This example shows a request that the miner start mining.

```
>> {"jsonrpc": "2.0", "id": 42, "method": "mining_start"}
<< {"jsonrpc": "2.0", "id": 42, "result": true}
```

### stop

The `start` method requests that the miner stop mining.  The method requires no arguments and returns a _bool_, `true` to indicate that the request has been received.

#### Example

This example shows a request that the miner stop mining.

```
>> {"jsonrpc": "2.0", "id": 43, "method": "mining_stop"}
<< {"jsonrpc": "2.0", "id": 43, "result": true}
```

### subscribe

The `subscribe` method is used to request notifications.  The method requires one argument, a _string_ indicating the desired source of the notifications, and returns a _string_, identifying the active subscription.  The following sources are supported:

- [hashRateSubscription](#hashratesubscription)
- [statusSubscription](#statussubscription)
- [submissionSubscription](#submissionsubscription)

If the request fails because the subscription does not exist, [a JSON-RPC error][json-rpc error specification] is returned, in keeping with the [JSON-RPC Specification][json-rpc specification].  The error will have a `code` of -32601, as implemented in `rpc/errors.go` of the [go-ethereum project][go-ethereum].

#### Examples

This example shows a successful subscription to the `hashRateSubscription` notification source.

```
>> {"jsonrpc": "2.0", "id": 201, "method": "mining_subscribe", "params": ["hashRateSubscription"]}
<< {"jsonrpc":"2.0","id":201,"result":"0xeaf8c028180b1b0eb3e8577f25d84e89"}
…
<< {"jsonrpc":"2.0","method":"mining_subscription","params":{"subscription":"0xeaf8c028180b1b0eb3e8577f25d84e89","result":4217.888140275206}}
```

This example shows an unsuccessful subscription to a non-existant notification source.

```
>> {"jsonrpc": "2.0", "id": 201, "method": "mining_subscribe", "params": ["doesNotExistSubscription"]}
<< {"jsonrpc":"2.0","id":201,"error":{"code":-32601,"message":"no \"doesNotExistSubscription\" subscription in mining namespace"}}
```

### unsubscribe

The `unsubscribe` method is used by a client to cancel a subscription to notifications.  The method requires one argument, a _string_ identifying the active subscription to be canceled, and returns a _bool_, indicating the success (_true_) of the request.  If the request fails because the subscription does not exist, [a JSON-RPC error][json-rpc error specification] is returned, in keeping with the [JSON-RPC Specification][json-rpc specification].  The error will have a `code` of -32600, as implemented in `rpc/errors.go` of the [go-ethereum project][go-ethereum].

#### Examples

This example shows a successful subscription and the subsequent cancelation of that subscription.

```
>> {"jsonrpc": "2.0", "id": 201, "method": "mining_subscribe", "params": ["hashRateSubscription"]}
<< {"jsonrpc":"2.0","id":201,"result":"0xeaf8c028180b1b0eb3e8577f25d84e89"}
…
<< {"jsonrpc":"2.0","method":"mining_subscription","params":{"subscription":"0xeaf8c028180b1b0eb3e8577f25d84e89","result":4217.888140275206}}
<< {"jsonrpc":"2.0","method":"mining_subscription","params":{"subscription":"0xeaf8c028180b1b0eb3e8577f25d84e89","result":4214.794689984841}}
…
>> {"jsonrpc": "2.0", "id": 202, "method": "mining_unsubscribe", "params": ["0xeaf8c028180b1b0eb3e8577f25d84e89"]}
<< {"jsonrpc":"2.0","id":202,"result":true}
```

This example shows the unsuccessful cancelation of a non-existant subscription.

```
>> {"jsonrpc": "2.0", "id": 201, "method": "mining_unsubscribe", "params": ["0x123456789abcdef0123456789abcdef0"]}
<< {"jsonrpc":"2.0","id":201,"error":{"code":-32000,"message":"subscription not found"}}
```

The error code is defined in `rpc/errors.go` of the [go-ethereum project][go-ethereum].

## Notifications

Clients subscribe to notifications using the ``subscribe`` method.  Clients may cancel a subscription with the ``unsubscribe`` method.  Subscriptions are automatically canceled when the client disconnects from the server.

### hashRateSubscription

The `hashRateSubscription` notification source reports the miner's current hash rate as a _JSON number_(in hashes per second) at a regular interval.  The interval is currently ten seconds and is defined in `func (c *Client) ReportHashRate()` in `../stratum/client.go`.

### statusSubscription

The `statusSubscription` notification source reports changes to the miner's mining status.  Specifically, notifications will be provided when the miner connects to the pool server and when the miner becomes disconnected from the pool server.  The status object is of the same type as the return value for the [getStatus](#getstatus) method.

### submissionSubscription

The `submissionSubscription` notification source reports the miner submitting a share to the pool server.  One notification is made for each share.

[json-rpc error specification]: https://www.jsonrpc.org/specification#error_object
[json-rpc specification]: https://www.jsonrpc.org/specification
[go-ethereum]: https://github.com/ethereum/go-ethereum
