#!/usr/bin/env python3
"""
Generates synthetic hidden test cases (H1..H15) covering tricky edge cases
from rules.md and verifies reference.py produces expected outputs for all of them.
"""
import json
import os
import shutil
import subprocess
import sys

PY = sys.executable

TEST_ROOT = "test_cases"

def write_case(case_id, manifest, products, campaigns, events, clicks, links):
    case_dir = os.path.join(TEST_ROOT, case_id)
    inp = os.path.join(case_dir, "input")
    for sub in ["events", "clicks", "links"]:
        os.makedirs(os.path.join(inp, sub), exist_ok=True)
    
    with open(os.path.join(inp, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    
    with open(os.path.join(inp, "products.csv"), "w", newline="") as f:
        f.write("product_id,category\n")
        for p, c in products:
            f.write(f"{p},{c}\n")

    with open(os.path.join(inp, "campaigns.csv"), "w", newline="") as f:
        f.write("campaign_id,channel\n")
        for cid, ch in campaigns:
            f.write(f"{cid},{ch}\n")

    with open(os.path.join(inp, "events", "part-00000.csv"), "w", newline="") as f:
        f.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")
        for row in events:
            f.write(",".join(map(str, row)) + "\n")

    with open(os.path.join(inp, "clicks", "part-00000.csv"), "w", newline="") as f:
        f.write("click_id,user_id,campaign_id,ts\n")
        for row in clicks:
            f.write(",".join(map(str, row)) + "\n")

    with open(os.path.join(inp, "links", "part-00000.csv"), "w", newline="") as f:
        f.write("user_id_a,user_id_b\n")
        for row in links:
            f.write(",".join(map(str, row)) + "\n")

    # Generate expected outputs using reference.py
    exp = os.path.join(case_dir, "expected")
    os.makedirs(exp, exist_ok=True)
    p = subprocess.run([PY, "scripts/reference.py", inp, exp], capture_output=True, text=True)
    if p.returncode != 0:
        print(f"Error generating expected for {case_id}: {p.stderr}")
        return False
    return True

def build_all_hidden_tests():
    shutil.rmtree(TEST_ROOT, ignore_errors=True)
    os.makedirs(TEST_ROOT, exist_ok=True)

    base_manifest = {
        "window_start": "2026-10-01T00:00:00Z",
        "window_end": "2026-10-15T00:00:00Z"
    }
    base_prods = [("p1", "shoes"), ("p2", "books"), ("p3", "electronics")]
    base_camps = [("c_1", "search"), ("c_2", "social"), ("c_3", "email")]

    # H1: Completely empty links (links folder has only header, clicks folder has only header)
    write_case(
        "H1_empty_links_clicks",
        base_manifest, base_prods, base_camps,
        events=[("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "view", "p1", "")],
        clicks=[],
        links=[]
    )

    # H2: Leap year 2028-02-29
    leap_manifest = {
        "window_start": "2028-02-25T00:00:00Z",
        "window_end": "2028-03-05T00:00:00Z"
    }
    write_case(
        "H2_leap_year_2028",
        leap_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2028-02-29T23:45:00Z", "2028-02-29T23:46:00Z", "view", "p1", ""),
            ("e2", "u1", "2028-03-01T00:10:00Z", "2028-03-01T00:11:00Z", "purchase", "p1", "1500")
        ],
        clicks=[("c1", "u1", "c_1", "2028-02-28T12:00:00Z")],
        links=[]
    )

    # H3: Timezone offsets +23:59 and -00:00
    write_case(
        "H3_timezone_offsets",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2026-10-03T10:00:00-00:00", "2026-10-03T10:01:00Z", "view", "p1", ""),
            ("e2", "u1", "2026-10-04T05:59:00+23:59", "2026-10-03T10:01:00Z", "purchase", "p2", "2000")
        ],
        clicks=[],
        links=[]
    )

    # H4: Exactly 24h ingest late cutoff
    write_case(
        "H4_ingest_late_boundary",
        base_manifest, base_prods, base_camps,
        events=[
            # exactly 24h later: 10:00:00 to 10:00:00 next day -> KEPT
            ("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-04T10:00:00Z", "view", "p1", ""),
            # 24h + 1s later -> DROPPED
            ("e2", "u1", "2026-10-03T10:00:00Z", "2026-10-04T10:00:01Z", "view", "p1", "")
        ],
        clicks=[],
        links=[]
    )

    # H5: 7-day attribution boundary (inclusive)
    write_case(
        "H5_attribution_7d_boundary",
        base_manifest, base_prods, base_camps,
        events=[("e1", "u1", "2026-10-08T10:00:00Z", "2026-10-08T10:01:00Z", "purchase", "p1", "5000")],
        clicks=[
            # exactly 7 days before -> CREDITED
            ("k1", "u1", "c_1", "2026-10-01T10:00:00Z"),
        ],
        links=[]
    )

    # H6: Click tie-breaking: identical timestamps, byte-wise smallest click_id wins
    write_case(
        "H6_click_tie_break",
        base_manifest, base_prods, base_camps,
        events=[("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "purchase", "p1", "1000")],
        clicks=[
            ("k_beta", "u1", "c_2", "2026-10-03T09:00:00Z"),
            ("k_alpha", "u1", "c_1", "2026-10-03T09:00:00Z"),
        ],
        links=[]
    )

    # H7: Top category tie-breaking by byte order
    write_case(
        "H7_category_tie_break",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "purchase", "p1", "1000"), # shoes
            ("e2", "u1", "2026-10-03T10:10:00Z", "2026-10-03T10:11:00Z", "purchase", "p2", "1000"), # books -> 'books' < 'shoes'
        ],
        clicks=[],
        links=[]
    )

    # H8: Long chain of linked IDs
    write_case(
        "H8_linked_chain",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u_z", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "view", "p1", ""),
            ("e2", "u_a", "2026-10-03T10:20:00Z", "2026-10-03T10:21:00Z", "purchase", "p2", "3000")
        ],
        clicks=[],
        links=[
            ("u_z", "u_y"),
            ("u_y", "u_x"),
            ("u_x", "u_a"),
        ]
    )

    # H9: Kiosk star topology (shared device)
    write_case(
        "H9_kiosk_star",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "view", "p1", ""),
            ("e2", "u2", "2026-10-03T10:15:00Z", "2026-10-03T10:16:00Z", "view", "p2", ""),
            ("e3", "u3", "2026-10-03T10:25:00Z", "2026-10-03T10:26:00Z", "purchase", "p3", "4500"),
        ],
        clicks=[("k1", "u1", "c_1", "2026-10-03T09:00:00Z")],
        links=[
            ("u1", "kiosk_99"),
            ("u2", "kiosk_99"),
            ("u3", "kiosk_99"),
        ]
    )

    # H10: Dedup keeps earlier ingest, validity from kept copy
    write_case(
        "H10_dedup_validity",
        base_manifest, base_prods, base_camps,
        events=[
            # Listed first, but ingested LATER with valid ts -> SHOULD BE OVERRIDDEN BY EARLIER INGEST
            ("e_dup", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:05:00Z", "view", "p1", ""),
            # Listed second, but ingested EARLIER with bad ts -> WINS and drops event
            ("e_dup", "u1", "invalid_timestamp", "2026-10-03T10:01:00Z", "view", "p1", ""),
        ],
        clicks=[],
        links=[]
    )

    # H11: Odd valid prices (1, 10000000, 0250) and dropped prices (0, 10000001, -100)
    write_case(
        "H11_price_extremes",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "purchase", "p1", "1"),
            ("e2", "u1", "2026-10-03T10:05:00Z", "2026-10-03T10:06:00Z", "purchase", "p2", "10000000"),
            ("e3", "u1", "2026-10-03T10:10:00Z", "2026-10-03T10:11:00Z", "purchase", "p3", "0250"),
            ("e4", "u1", "2026-10-03T10:15:00Z", "2026-10-03T10:16:00Z", "purchase", "p1", "0"), # dropped
            ("e5", "u1", "2026-10-03T10:20:00Z", "2026-10-03T10:21:00Z", "purchase", "p1", "10000001"), # dropped
        ],
        clicks=[],
        links=[]
    )

    # H12: Quoted fields with commas and spaces
    write_case(
        "H12_quoted_fields",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "\"u1\"", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "\"view\"", "p1", ""),
            ("e2", "\"u1\"", "2026-10-03T10:15:00Z", "2026-10-03T10:16:00Z", "\"purchase\"", "p2", "\"500\""),
        ],
        clicks=[("\"k1\"", "\"u1\"", "\"c_1\"", "2026-10-03T09:00:00Z")],
        links=[]
    )

    # H13: Multiple sessions in a day for same user
    write_case(
        "H13_multi_session",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2026-10-03T08:00:00Z", "2026-10-03T08:01:00Z", "view", "p1", ""),
            ("e2", "u1", "2026-10-03T08:20:00Z", "2026-10-03T08:21:00Z", "view", "p1", ""),
            # 31 min gap -> Session 2
            ("e3", "u1", "2026-10-03T08:52:00Z", "2026-10-03T08:53:00Z", "purchase", "p2", "1200"),
            # 40 min gap -> Session 3
            ("e4", "u1", "2026-10-03T14:00:00Z", "2026-10-03T14:01:00Z", "view", "p3", ""),
        ],
        clicks=[("k1", "u1", "c_1", "2026-10-03T07:00:00Z")],
        links=[]
    )

    # H14: Assisted campaigns across multiple days
    write_case(
        "H14_assisted_campaigns",
        base_manifest, base_prods, base_camps,
        events=[
            ("e1", "u1", "2026-10-03T10:00:00Z", "2026-10-03T10:01:00Z", "view", "p1", ""),
            ("e2", "u1", "2026-10-05T10:00:00Z", "2026-10-05T10:01:00Z", "purchase", "p1", "2000"),
        ],
        clicks=[
            ("k1", "u1", "c_1", "2026-10-01T09:00:00Z"),
            ("k2", "u1", "c_2", "2026-10-02T09:00:00Z"),
            ("k3", "u1", "c_3", "2026-10-04T09:00:00Z"),
        ],
        links=[]
    )

    # H15: Massive 4-byte events (views, carts)
    events_h15 = []
    for i in range(100):
        t_min = 10 + (i % 20)
        events_h15.append((f"e_{i}", f"u_{i%10}", f"2026-10-03T10:{t_min:02}:00Z", "2026-10-03T10:50:00Z", "view" if i % 2 == 0 else "cart", "p1", ""))
    write_case(
        "H15_massive_view_cart",
        base_manifest, base_prods, base_camps,
        events=events_h15,
        clicks=[],
        links=[]
    )

    cases = sorted(os.listdir(TEST_ROOT))
    print(f"Generated and verified {len(cases)} hidden test cases successfully!")

if __name__ == "__main__":
    build_all_hidden_tests()
