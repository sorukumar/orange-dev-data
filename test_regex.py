import re

content = """
## Notable code and documentation changes

- [Bitcoin Core #35889][] changes the `gettxspendingprevout` RPC...

- [Eclair #3366][] hardens [splicing][topic splicing] against peers...

- [LND #11090][] rate limits inbound `ping` messages...
"""

pattern = re.compile(r'(?:^|\n)[-*]\s+\[([^\]]+?)\s+#(\d+)\](?:\[\]|\([^\)]*\))\s*(.*?)(?=\n[-*]\s+\[|\Z)', re.IGNORECASE | re.DOTALL)
for match in pattern.finditer(content):
    print("Repo:", match.group(1))
    print("PR:", match.group(2))
    print("Desc:", match.group(3)[:50] + "...")
