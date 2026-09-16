---
name: pseudocode-pr
description: Use whenever the user asks to change, edit, fix, or extend pseudo.txt (or any pseudocode in this repo). Instead of editing on main, create a branch, commit the change, push it, and open a GitHub PR for review.
---

# Pseudocode PR workflow

Every change to pseudocode in this repo goes through a branch + PR, never a direct commit to `main`. This gives the user a reviewable diff before anything lands.

## Steps

1. **Confirm the change first.** Understand exactly what pseudocode edit is being requested before touching files.
2. **Sync main and branch.**
   ```
   git checkout main
   git pull
   git checkout -b pseudocode/<short-kebab-description>
   ```
   Pick `<short-kebab-description>` from the change itself (e.g. `pseudocode/retry-states`, `pseudocode/lock-chain-cleanup`).
3. **Make the edit(s)** to the pseudocode file(s) (e.g. `pseudo.txt`) using Edit/Write as normal.
4. **Commit** with a message describing *why* the pseudocode changed, not just what:
   ```
   git add pseudo.txt
   git commit -m "$(cat <<'EOF'
   <concise summary of the pseudocode change>

   Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
   EOF
   )"
   ```
5. **Push the branch**:
   ```
   git push -u origin pseudocode/<short-kebab-description>
   ```
6. **Open a PR** with `gh pr create`, always writing a body that explains:
   - **What changed** in the pseudocode (which procedure/data structure, what was added/removed/modified)
   - **Why** the change was made (the reasoning or problem it addresses)
   - **What to review** — anything the user should double check (e.g. a TODO resolved, an edge case introduced)

   ```
   gh pr create --base main --head pseudocode/<short-kebab-description> \
     --title "<short imperative title>" \
     --body "$(cat <<'EOF'
   ## What changed
   - <bullet per pseudocode change>

   ## Why
   <1-3 sentences>

   ## Review notes
   - <anything specific to double check>

   🤖 Generated with [Claude Code](https://claude.com/claude-code)
   EOF
   )"
   ```
7. **Report back** to the user: the branch name, a one-line summary of the change, and the PR URL returned by `gh pr create`. Do not merge it yourself.

## Rules

- Never commit pseudocode changes directly to `main`.
- One branch/PR per logical pseudocode change; don't bundle unrelated edits.
- If already on a non-main branch when asked for a new, unrelated pseudocode change, still branch off `main`, not off the current branch.
- If `gh` is not authenticated (`gh auth status` fails), stop and tell the user to run `gh auth login` before continuing.
