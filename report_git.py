import subprocess
import requests
from collections import defaultdict, Counter
import os
import re
from datetime import datetime, timedelta

# Config
import sys

scope = sys.argv[1] if len(sys.argv) > 1 else "weekly"

if scope == "weekly":
    SINCE = '--since="1 week ago"'
elif scope == "monthly":
    SINCE = '--since="1 month ago"'
elif scope == "yearly":
    SINCE = '--since="1 year ago"'
elif scope == "all":
    SINCE = ""
else:
    print(f"❌ Unknown scope: {scope}. Use 'weekly', 'monthly', 'yearly', or 'all'.")
    sys.exit(1)

scope=scope.capitalize()
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1403424863783227532/Ltc-F2WQJvHnvmGID61BstnICwsTOEbNU_HJYUHG6gpBEtd1rY-bJDpiIdXzytpz3cTP"


mailmap_cache = {}

def resolve_mailmap(name, email):
    contact = f"{name} <{email}>"
    print(f"Resolving mailmap for: {contact}")
    if contact in mailmap_cache:
        return mailmap_cache[contact]
    try:
        result = subprocess.run(
            ['git', 'check-mailmap', contact],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True
        )
        resolved = result.stdout.strip()
        # Extract name only (before the first '<')
        resolved_name = resolved.split('<')[0].strip() if resolved else name
        mailmap_cache[contact] = resolved_name
        
        print(f"Resolved to: {resolved_name}")
        return resolved_name
    except Exception:
        mailmap_cache[contact] = name
        return name


# Git command with commit marker
cmd = f'git log {SINCE} --pretty=format:"--COMMIT--%n%h|%an|%ae|%s%n%b" --numstat'
result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, shell=True)
blocks = result.stdout.split('--COMMIT--\n')

author_commits = defaultdict(int)
total_additions = 0
total_deletions = 0
changed_files = set()
merged_prs = []

for block in blocks:
    lines = block.strip().splitlines()
    if not lines:
        continue

    try:
        commit_hash, name, email, subject = lines[0].split('|', 3)
        author = resolve_mailmap(name.strip(), email.strip())
        author_commits[author] += 1
    except ValueError:
        continue

    body_lines = []
    stat_lines = []

    for line in lines[1:]:
        if re.match(r'^\d+\s+\d+\s+.+$', line):
            stat_lines.append(line)
        else:
            body_lines.append(line)

    body = "\n".join(body_lines)

    # Co-authors
    coauthors = re.findall(r'Co-authored-by:\s*(.+?)\s*<(.+?)>', body)
    resolved_coauthors = []
    for cname, cemail in coauthors:
        coauthor = resolve_mailmap(cname.strip(), cemail.strip())
        author_commits[coauthor] += 1
        resolved_coauthors.append(coauthor)

    # PR detection
    pr_number = None
    pr_branch = None
    match_subject = re.search(r'(?:Merge pull request #|#)(\d+)(?: from ([\w\-/]+))?', subject)
    if match_subject:
        pr_number = match_subject.group(1)
        pr_branch = match_subject.group(2) if match_subject.group(2) else None
    else:
        match_body = re.search(r'(?:Closes|Fixes|Resolves)\s+#(\d+)', body)
        if match_body:
            pr_number = match_body.group(1)

    if pr_number:
        merged_prs.append((commit_hash, pr_number, pr_branch, author, resolved_coauthors, subject))

    # File stats
    for stat in stat_lines:
        match = re.match(r'^(\d+)\s+(\d+)\s+(.+)$', stat)
        if match:
            additions = int(match.group(1))
            deletions = int(match.group(2))
            file = match.group(3).strip()
            total_additions += additions
            total_deletions += deletions
            changed_files.add(file)

# Extension summary
ext_counts = Counter()
for f in changed_files:
    ext = os.path.splitext(f)[1].lower().strip()
    ext = re.sub(r'[^\w.]+$', '', ext) or "(no_ext)"
    ext_counts[ext] += 1

# Build summary
summary = f"🧾 **PackOS {scope} Git Summary**\n----------------------"
for author, count in sorted(author_commits.items()):
    summary += f"\n👤 **{author}**: {count} commits"

summary += f"\n\n➕ **Total additions**: {total_additions}"
summary += f"\n➖ **Total deletions**: {total_deletions}"

if ext_counts:
    summary += "\n📄 **Files changed by type:**\n" + "\n".join(
        f"• `{ext}`: {count}" for ext, count in sorted(ext_counts.items())
    )

if merged_prs:
    summary += "\n📦 **Merged or squash PRs:**"
    for commit_hash, pr_number, pr_branch, author, coauthors, subject in merged_prs:
        co_list = ", ".join(coauthors)
        summary += f"\n• PR #{pr_number} from `{pr_branch or '-'}` by **{author}**"
        if co_list:
            summary += f" (with co-authors: {co_list})"

print(summary)

# Post to Discord
payload = {"content": summary[:1999]} # Discord message limit
response = requests.post(DISCORD_WEBHOOK, json=payload)

if response.status_code == 204:
    print("✅ Summary posted to Discord.")
else:
    print(f"❌ Failed to post: {response.status_code} - {response.text}")
