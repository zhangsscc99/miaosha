#!/usr/bin/env python3
"""生成 1000 个 8KB 左右的静态商详页，并把商品快照写进 Redis（RESP 管道）。

静态页和 Java 动态页用同一套结构，唯一区别是：静态页里的库存/倒计时/价格是
生成时刻的快照，之后不会变；动态页每次请求都从 Redis 读一遍再拼。
"""
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOODS = int(os.environ.get("GOODS", "1000"))
STATIC_DIR = os.path.join(BASE, "runtime", "static")

PREFIX = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>限时秒杀 - 商品详情</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,"PingFang SC",sans-serif;background:#f5f5f5;color:#222;font-size:14px}
header{height:44px;line-height:44px;text-align:center;background:#e4393c;color:#fff;font-size:16px}
.gallery{height:180px;background:linear-gradient(135deg,#ffe6e6,#ffd0d0);display:flex;align-items:center;justify-content:center;color:#c33}
.box{background:#fff;margin:8px;border-radius:8px;padding:12px}
h1{font-size:17px;line-height:1.45;font-weight:600}
.price{margin-top:8px}
.now{color:#e4393c;font-size:26px;font-weight:700}
.was{color:#999;text-decoration:line-through;margin-left:8px;font-size:12px}
.tip{background:#e4393c;color:#fff;font-size:11px;padding:2px 6px;border-radius:3px;margin-left:8px}
.stock{margin-top:8px;color:#666}
.stock b{color:#e4393c;font-size:18px}
.countdown{margin-top:6px;color:#e4393c;font-size:13px}
#buy{width:100%;height:46px;margin-top:12px;border:0;border-radius:23px;background:linear-gradient(90deg,#ff6a00,#e4393c);color:#fff;font-size:17px;font-weight:600}
.spec table{width:100%;border-collapse:collapse}
.spec td{border-bottom:1px solid #f0f0f0;padding:8px 4px;font-size:12px}
.spec .k{color:#999;width:96px}
.spec .v{color:#333}
.reviews .r{border-bottom:1px solid #f5f5f5;padding:8px 0}
.reviews .u{color:#2b6cb0;font-size:12px}
.reviews p{color:#555;font-size:12px;line-height:1.6;margin-top:4px}
footer{text-align:center;color:#aaa;font-size:11px;padding:16px 0 28px}
</style></head><body>
<header>限时秒杀</header>
<div class="gallery"><img src="https://img.example.com/p/placeholder.jpg" alt="商品图" width="140" height="140"></div>
<div class="box">
"""

SUFFIX = """</div>
<footer>秒杀商品由平台自营发货 · 价格以结算页为准</footer>
<script>
function buy(){document.getElementById('buy').disabled=true;document.getElementById('buy').innerText='排队中...';}
var left=3600;setInterval(function(){left=left>0?left-1:0;var h=Math.floor(left/3600),m=Math.floor(left%3600/60),s=left%60;document.getElementById('cd').innerText=(h<10?'0':'')+h+':'+(m<10?'0':'')+m+':'+(s<10?'0':'')+s;},1000);
</script>
</body></html>
"""


def page(gid: int) -> str:
    price = 99 + (gid * 37) % 900
    stock = 100 + (gid * 13) % 900
    rows = "".join(
        f'<tr><td class="k">参数 {i}</td><td class="v">规格值-{i} / 批次 {2026 + i % 2} / 产地 杭州</td></tr>'
        for i in range(1, 41)
    )
    reviews = "".join(
        f'<div class="r"><span class="u">用户{1000 + gid * 7 + i}</span>'
        f'<p>质量不错，物流很快，第 {i} 次回购了，价格比平时便宜不少。</p></div>'
        for i in range(1, 11)
    )
    return (
        PREFIX
        + f'<h1 id="title">2026 新款 秒杀专供 商品 {gid} 号</h1>'
        + f'<div class="price"><span class="now">￥{price}.00</span>'
        + f'<span class="was">￥{price * 24 // 10}.00</span><span class="tip">限时秒杀</span></div>'
        + f'<div class="stock">剩余 <b id="stock">{stock}</b> 件 · 已抢 {88000 - stock} 件</div>'
        + '<div class="countdown">距结束 <b id="cd">00:59:59</b></div>'
        + '<button id="buy" onclick="buy()">立即抢购</button>'
        + f'<div class="spec"><table><tbody>{rows}</tbody></table></div>'
        + f'<div class="reviews">{reviews}</div>'
        + SUFFIX
    )


def resp(*parts: str) -> bytes:
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        b = str(p).encode()
        out.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
    return b"".join(out)


def main() -> None:
    os.makedirs(STATIC_DIR, exist_ok=True)
    seed = bytearray()
    sizes = []
    for gid in range(1, GOODS + 1):
        html = page(gid)
        sizes.append(len(html.encode()))
        with open(os.path.join(STATIC_DIR, f"p{gid}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        price = 99 + (gid * 37) % 900
        stock = 100 + (gid * 13) % 900
        seed += resp("SET", f"item:{gid}",
                     f'{{"title":"2026 新款 秒杀专供 商品 {gid} 号","price":"{price}.00","stock":{stock}}}')
    with open(os.path.join(BASE, "runtime", "seed.resp"), "wb") as f:
        f.write(seed)
    print(f"goods={GOODS} files={len(sizes)} page_bytes_min={min(sizes)} "
          f"max={max(sizes)} avg={sum(sizes) // len(sizes)} total_mb={sum(sizes) / 1048576:.2f}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--size-only":
        s = [len(page(i).encode()) for i in range(1, 51)]
        print(min(s), max(s), sum(s) // len(s))
    else:
        main()
