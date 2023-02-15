# BNA – whale tracker Discord bot for Idena

This is a Discord bot for the Idena project. Its main purpose is to show notifications about movements of iDNA coins, CEX and DEX trades, and various on-chain events. It also has commands that you can run in Discord to show recent on-chain and market stats.

## Features
* Notifications:
    * For large iDNA transfers on the native Idena chain and on BSC
    * For transfers by interesting addresses (e.g. developers' wallets)
    * For native chain events:
        * Terminations of old identities or identities with large stake
        * Mass identity changes by pools (terminations, delegations, undelegations)
    * For large DEX volume on BSC (trades and liquidity changes)
    * For large CEX volume (Hotbit, BitMart, ProBit, ViteX)
    * For when an identity enters a "stake club" (like 100k stake club, etc.)
* Commands:
    * For recent chain and market stats
    * For recent pool changes
    * For top recent events of some type (transfers, DEX trades, stake replenishments, terminations)
    * For showing the rank of identity according to its stake and age
* Lists of transactions/identities can be moved through with buttons
* Notifications for recent events of a similar type to a new event get updated in place
* Built in command rate limiting and per-command authorization
* Most configuration is done using commands by authorized users

## Screenshots
Notifications | Commands
------------|-----------
DEX transactions</br> <img src="/screenshots/dex.png?raw=true" height="120"> | Chain and market stats</br> <img src="/screenshots/cmd_stats.png?raw=true" height="120">
DEX or CEX volume</br> <img src="/screenshots/dex_multi.png?raw=true" width="50"> <img src="/screenshots/cex_trade.png?raw=true" width="40"> | Top DEX transactions</br> <img src="/screenshots/cmd_top_dex.png?raw=true" width="100">
Transfers by interesting addresses </br> <img src="/screenshots/interesting.png?raw=true" width="100">| Top transfers </br> <img src="/screenshots/cmd_top_transfer.png?raw=true" width="100">
Mass pool changes</br> <img src="/screenshots/mass_termination.png?raw=true" width="100"> | Pool stats</br> <img src="/screenshots/cmd_pools.png?raw=true" width="100">
Stake club membership</br> <img src="/screenshots/club.png?raw=true" width="100"> | Identity rank</br> <img src="/screenshots/cmd_rank.png?raw=true" width="100">

and more in the `screenshots` folder.

## Running the bot
### Requirements

Must have:
* 2GB of RAM, reliable internet connection
* Discord app with bot functionality enabled
* BSC endpoint (HTTP and WS, I recommend QuickNode - their free plan is enough)

Can be installed by Docker automatically:
* Idena node (regular non-indexer node, no identity required)
* PostgreSQL database
* Python 3.10

### Installing and running
#### Using Docker:

1. Fill out the `env_docker` file
2. Install and run:
```
docker compose up
```
The initial node synchronization may take a very long time. You can speed it up by enabling `FastSync` in `docker_idena/idena-conf.json` (which disables the full node) and rebuilding the image, or by putting a complete snapshot of `idenachain.db` into node's volume.

#### Or manually:

0. Install and start PostgreSQL and an Idena node
1. Fill out the `env` file
2. Install required libraries:
```
pip install -r requirements.txt
```
3. Run the bot:
```
python main.py -env env
```

### First time setup
1. Add your bot to a server using a link like this: https://discord.com/oauth2/authorize?client_id=YOUR_APP_ID&scope=bot&permissions=3072
2. As the dev user (which is specified in an env file):
    1. Run `/change_admins` command to set users/roles that can run settings commands
3. As an admin (or as the dev user):
    1. Run `/change_command_roles` command to set roles which can run general commands
    2. Run `/notification_channel` command to set the channel where event notifications will be sent
    3. Adjust thresholds for events using other commands

## TODO
* Show accolades next to addresses (notable stake, age, or pool size)
* Parse contract calls and multisig movements
* Tracking oracles and creating relevant events
* Handle known addresses in a better way
* Use an indexer node instead of a regular node

## Inner workings
If you ever want to modify something, here's a brief overview. The project is structured in three parts: listeners, tracker, and the Discord bot. All the parts (and their subparts) run as asynchronous tasks and communicate events via queues.

* Listeners (`idena_listener`, `bsc_listener`, `cex_listeners`) connect to their data sources, generate events like `Transfer` and `Trade`, and send them to the tracker.
* Tracker receives events from listeners, inserts them into the database, and generates user-facing events if needed.
* Discord bot receives events from the tracker and formats them into messages. It also calls methods on the tracker to get data to generate command responses.

## Attributions
Libraries used:
* [aiohttp](https://github.com/aio-libs/aiohttp) - Apache License Version 2.0, Copyright aio-libs contributors
* [colorlog](https://github.com/revolunet/colorlog) - MIT License, Copyright (c) 2012 Sam Clements
* [dacite](https://github.com/konradhalas/dacite) - MIT License, Copyright (c) 2018 Konrad Hałas
* [disnake](https://github.com/DisnakeDev/disnake) - MIT License, Copyright (c) 2021-present Disnake Development
* [psycopg](https://github.com/psycopg/psycopg) - LGPL-3.0 License, Copyright (C) 2020 The Psycopg Team
* [pytest](https://github.com/pytest-dev/pytest) - MIT License, Copyright (c) 2004 Holger Krekel and others
* [sortedcontainers](https://pypi.org/project/sortedcontainers) - Apache License Version 2.0, Copyright 2014-2019 Grant Jenks
* [websockets](https://github.com/aaugustin/websockets) - BSD 3-Clause License, Copyright (c) Aymeric Augustin and contributors

### Copyright and license
This program is released under the MIT License (see LICENSE file).

Copyright © 2023 bus.
