#!/usr/bin/env python3
"""Render .puml files to PNG via the PlantUML public server.

Usage: python3 render.py [file.puml ...]   (default: all *.puml in this dir)
"""
import glob
import sys
import os
from plantuml import PlantUML

server = PlantUML(url="http://www.plantuml.com/plantuml/img/")

if __name__ == "__main__":
    files = sys.argv[1:] or sorted(glob.glob(os.path.join(os.path.dirname(__file__), "*.puml")))
    for f in files:
        url = server.get_url(open(f, encoding="utf-8").read())
        out = os.path.splitext(f)[0] + ".png"
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as r, open(out, "wb") as w:
            w.write(r.read())
        print(out)
