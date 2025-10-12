# Spacebar Bridge
A bot that serves as message bridge between Discord and Spacebar servers. But can also be used to bridge Discord to Discord and Spacebar to Spacebar servers.


## Features
- 2-way message forwardingr
- Forward full message content: atachments, embeds, stickers, emojis, mentions, polls interactions (non-interactive)
- Custom bot status (Discord only)


## Setup
1. Install Python 3.12 or later
2. Clone this repository
3. Install [uv](https://docs.astral.sh/uv/getting-started/installation/)
4. `cd spacebar-bridge`
5. Configure bridge, open `config.json`
    1. Create bot on discord
        1. Obtain bot token and paste it on `config.json`
        2. Under Bot tab enable "Message Content Intent"
        3. Create invite (OAuth tab) with scope "bot" and perms:  
            View Channels, Send Messages, Read Message History, Add reactions, Use Embedded Activities
        4. Open generated url and select server to add bot
    2. Create bot on spacebar
        1. Obtain bot token and paste it on `config.json`
        2. Open bot Invite Creator and select:  
            Add Reactions, View Channels, Send Messages, Embed Links, Attach Files, Read Message History, Use Activities
    3. Add bridge guild IDs to `config.json`
    4. Add bridge channel IDs to `config.json`
6. run main script: `uv run main.py`
7. Check `spacebar_bridge.log` for any errors.
8. `Ctrl+C` to stop bridge.
9. To set "debug" log level, run `export LOG_LEVEL=DEBUG ` before starting the bridge.

### Database options
`dir_path` - where will databases be stored  
`cleanup_days` - interval in days between database cleanups, set to `null` to disable cleanup  
`pair_lifetime_days` - how long will each pair be kept in database before its removed, set to `null` to disable cleanup  

## TODO
- Edit message
- Delete message
- Reply messages
- Reactions
- Run on multiple guilds
