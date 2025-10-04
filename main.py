import json
import logging
import signal
import sys
import threading
import time

from bridge import discord, gateway

logger = logging
logging.basicConfig(
    level="INFO",
    filename="spacebar_bridge.log",
    encoding="utf-8",
    filemode="w",
    format="{asctime} - {levelname}\n  [{module}]: {message}\n",
    style="{",
    datefmt="%Y-%m-%d-%H:%M:%S",
)
ERROR_TEXT = "\nUnhandled exception occurred. Please report here: https://github.com/mzivic7/spacebar-bridge/issues"


def get_author_name(message):
    """Get author name from message"""
    if message["nick"]:
        return message["nick"]
    if message["global_name"]:
        return message["global_name"]
    if message["username"]:
        return message["username"]
    return "Unknown"


def get_author_pfp(message, cdn_url, size=80):
    """Get author pfp url from message"""
    avatar_id = message["avatar_id"]
    if avatar_id:
        return f"https://{cdn_url}/avatars/{message["user_id"]}/{avatar_id}.webp?size={size}"
    return None


class Bridge:
    """Bridge class"""

    def __init__(self):
        with open("config.json", "r") as f:
            config = json.load(f)
        self.run = True

        host_a = config["discord"]["host"]
        self.cdn_a = config["discord"]["cdn_host"]
        token_a = config["discord"]["token"]
        host_b = config["spacebar"]["host"]
        self.cdn_b = config["spacebar"]["cdn_host"]
        token_b = config["spacebar"]["token"]
        bridges = config["bridges"]

        custom_status = config["custom_status"]
        custom_status_emoji = config["custom_status_emoji"]

        self.guild_id_a = config["discord_guild_id"]
        self.guild_id_b = config["spacebar_guild_id"]

        self.channels_a = []
        self.bridges_a = {}
        self.channels_b = []
        self.bridges_b = {}
        for bridge in bridges:
            a = bridge["discord_channel_id"]
            b = bridge["spacebar_channel_id"]
            self.channels_a.append(a)
            self.bridges_a[a] = b
            self.channels_b.append(b)
            self.bridges_b[b] = a

        self.discord_a = discord.Discord(token_a, host_a, self.cdn_a, "Discord")
        self.gateway_a = gateway.Gateway(token_a, host_a, "Discord")
        self.gateway_a.connect()
        self.discord_b = discord.Discord(token_b, host_b, self.cdn_b, "Spacebar")
        self.gateway_b = gateway.Gateway(token_b, host_b, "Spacebar", compressed=False)
        self.gateway_b.connect()

        while not (self.gateway_a.get_ready() and self.gateway_b.get_ready()):
            if self.gateway_a.error:
                logger.fatal(f"Gateway A error: \n {self.gateway_a.error}")
                sys.exit(self.gateway_a.error + ERROR_TEXT)
            if self.gateway_b.error:
                logger.fatal(f"Gateway B error: \n {self.gateway_b.error}")
                sys.exit(self.gateway_b.error + ERROR_TEXT)
            if not self.gateway_a.run or not self.gateway_b.run:
                sys.exit()
            time.sleep(0.2)

        self.my_id_a = self.gateway_a.get_my_id()
        self.my_id_b = self.gateway_b.get_my_id()

        self.gateway_a.update_presence(
            status="online",
            custom_status=custom_status,
            custom_status_emoji=custom_status_emoji,
        )
        # self.gateway_b.update_presence(   # not supported by spacebar
        #     status="online",
        #     custom_status=custom_status,
        #     custom_status_emoji=custom_status_emoji,
        # )

        logger.info("Bridge initialized successfully")

        threading.Thread(target=self.loop_b, daemon=True).start()
        self.loop_a()


    def loop_a(self):
        """Loop A"""
        while self.run:

            # get messages
            while self.run:
                new_message = self.gateway_a.get_messages()
                if new_message:
                    data = new_message["d"]
                    if data["channel_id"] in self.channels_a and data.get("user_id") != self.my_id_a:
                        op = new_message["op"]
                        if op == "MESSAGE_CREATE":
                            source_channel = data["channel_id"]
                            target_channel = self.bridges_a[source_channel]
                            source_id = data["id"]
                            author_name = get_author_name(data)
                            author_pfp = get_author_pfp(data, self.cdn_a)
                            content = data["content"]
                            self.message_send_b_noembed(   # spacebar bug with embeds
                                target_channel,
                                author_name,
                                author_pfp,
                                content,
                            )
                            logger.debug(f"CREATE: {source_channel} > {target_channel} = [{author_name}] - ({source_id}) - {content}")
                        elif op == "MESSAGE_UPDATE":
                            pass
                        elif op == "MESSAGE_DELETE":
                            pass
                        elif op == "MESSAGE_REACTION_ADD":
                            pass
                        elif op == "MESSAGE_REACTION_REMOVE":
                            pass

                else:
                    break

            # check gateway for errors
            if self.gateway_a.error:
                logger.fatal(f"Gateway error: \n {self.gateway_a.error}")
                sys.exit(self.gateway_a.error + ERROR_TEXT)

            time.sleep(0.1)   # some reasonable delay
        self.run = False


    def loop_b(self):
        """Loop B"""
        while self.run:

            # get messages
            while self.run:
                new_message = self.gateway_b.get_messages()
                if new_message:
                    data = new_message["d"]
                    if data["channel_id"] in self.channels_b and data.get("user_id") != self.my_id_b:
                        op = new_message["op"]
                        if op == "MESSAGE_CREATE":
                            source_channel = data["channel_id"]
                            target_channel = self.bridges_b[source_channel]
                            source_id = data["id"]
                            author_name = get_author_name(data)
                            author_pfp = get_author_pfp(data, self.cdn_b)
                            content = data["content"]
                            self.message_send_a(
                                target_channel,
                                author_name,
                                author_pfp,
                                content,
                            )
                            logger.debug(f"CREATE: {source_channel} > {target_channel} = [{author_name}] - ({source_id}) - {content}")
                        elif op == "MESSAGE_UPDATE":
                            pass
                        elif op == "MESSAGE_DELETE":
                            pass
                        elif op == "MESSAGE_REACTION_ADD":
                            pass
                        elif op == "MESSAGE_REACTION_REMOVE":
                            pass

                else:
                    break

            # check gateway for errors
            if self.gateway_a.error:
                logger.fatal(f"Gateway error: \n {self.gateway_b.error}")
                sys.exit(self.gateway_b.error + ERROR_TEXT)

            time.sleep(0.1)   # some reasonable delay
        self.run = False


    def message_send_a(self, channel_id, author_name, author_pfp, message_text):
        """Send message A"""
        if not message_text:
            message_text = "*Unknown message content*"
        embeds = [{
            "author": {
                "name": author_name,
            },
            "description": message_text,
        }]
        if author_pfp:
            embeds[0]["author"]["icon_url"] = author_pfp
        self.discord_a.send_message(
            channel_id=channel_id,
            message_text="",
            reply_id=None,
            reply_channel_id=None,
            reply_guild_id=None,
            reply_ping=True,
            embeds=embeds,
        )


    def message_send_b(self, channel_id, author_name, author_pfp, message_text):
        """Send message B"""
        if not message_text:
            message_text = "*Unknown message content*"
        embeds = [{
            "author": {
                "name": author_name,
            },
            "description": message_text,
        }]
        if author_pfp:
            embeds[0]["author"]["icon_url"] = author_pfp
        self.discord_b.send_message(
            channel_id=channel_id,
            message_text="",
            reply_id=None,
            reply_channel_id=None,
            reply_guild_id=None,
            reply_ping=True,
            embeds=embeds,
        )


    def message_send_b_noembed(self, channel_id, author_name, _, message_text):
        """Send message B without using embeds"""
        content = f"{author_name}:\n{message_text}"
        self.discord_b.send_message(
            channel_id=channel_id,
            message_text=content,
            reply_id=None,
            reply_channel_id=None,
            reply_guild_id=None,
            reply_ping=True,
        )


def sigint_handler(_signum, _frame):
    """Handling Ctrl-C event"""
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    bridge = Bridge()
