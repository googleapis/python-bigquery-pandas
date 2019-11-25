import re


with open("docs/source/changelog.rst") as f:
    text = f.read()

# :issue:`312` -> https://github.com/googleapis/python-bigquery-pandas/issues/312
c = re.compile(r":issue:`([0-9]+)`", flags=re.MULTILINE)
print(re.sub(c, r"[#\1](https://github.com/googleapis/python-bigquery-pandas/issues/\1)", text))
