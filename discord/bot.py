import os
import random
from pairs import Pairs
import discord
from dotenv import load_dotenv
import re

load_dotenv()
TOKEN = os.getenv('TOKEN')

client = discord.Client()

@client.event
async def on_ready():
    print(f'{client.user.name} has connected to Discord!')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    content = message.content
    if content[0:5].lower() == 'pairs':
        tickers = re.search('([a-z A-Z])+', content[6:])
        weights = dict()
        ticker_list = tickers.group(0).split()
        tmp0 = sum(len(i) for i in ticker_list)
        weight_list = [float(j) for j in content[6+tmp0+len(ticker_list):].split()]

        lookback = int(weight_list.pop(len(weight_list) - 1))
        for i in range(len(ticker_list)):
            weights[ticker_list[i]] = weight_list[i]

        p = Pairs()
        data, pct_ret, text = p.index(weights, lookback_window=lookback)

        with open('index.png', 'rb') as f:
           picture = discord.File(f)
           await message.channel.send(file=picture)

        p.delete_data()

        await message.channel.send(text)


client.run(TOKEN)
