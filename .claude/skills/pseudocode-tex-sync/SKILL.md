---
name: pseudocode-tex-sync
description: Fires automatically (via a PostToolUse hook on pseudo.txt) whenever pseudo.txt is edited. Also invoke manually whenever pseudo.txt and main.tex have drifted. Translates pseudo.txt into the algorithmicx LaTeX already used in main.tex so the PDF (compiled on Overleaf) stays in sync with the pseudocode, without changing the underlying system design.
---

# Pseudocode -> LaTeX sync

`main.tex` is a LaTeX rendering of `pseudo.txt` using the `algorithm`/`algorithmicx` packages,
split into three `algorithm` environments (Part 1: Data Structures & Execution, Part 2: Access &
Locking, Part 3: Commit Protocol). Every time `pseudo.txt` changes, `main.tex` must be updated so
the two never drift — the LaTeX doc is what gets compiled on Overleaf into the PDF the team reads.

This skill only transcribes; it never changes what the algorithm does. If a change in `pseudo.txt`
looks like a substantive protocol change (not just formatting), still transcribe it faithfully —
this skill is not the place to judge or fix the pseudocode logic. Load [[co-shim-context]] if you
need project background to understand what a line means.

## Workflow

1. **Diff the two files.** Read `pseudo.txt` in full and locate the corresponding procedure(s) /
   data structures in `main.tex`. Use `git diff pseudo.txt` (if there's a pending diff) to see
   exactly what changed, rather than re-deriving the whole file from scratch.
2. **Map each changed pseudo.txt construct to algorithmicx** using the translation table below.
3. **Edit main.tex in place** — same three-algorithm split, same procedure order, same style as
   the surrounding untouched lines. Don't restructure or reformat parts that didn't change.
4. **Sanity-check LaTeX validity** (see Overleaf checklist below) before finishing.
5. **Report** which procedures/data structures changed and a one-line note on anything ambiguous
   you had to judge-call (e.g. how to render a new symbol).

## Translation table (pseudo.txt -> algorithmicx)

| pseudo.txt | main.tex (algorithmicx) |
|---|---|
| `procedure NAME(args):` | `\Procedure{NAME}{args}` ... `\EndProcedure` |
| indentation levels | one `\State` per line; nested blocks indent visually via `\indent` inside `\State` text (matches existing style, e.g. inside `atomic(...) { }` blocks) |
| `if COND:` / `else if COND:` / `else:` | `\If{COND}` / `\ElsIf{COND}` / `\Else` ... `\EndIf` |
| `for each X in Y:` | `\For{\textbf{each} $X \in Y$}` ... `\EndFor` |
| `while COND:` | `\While{COND}` ... `\EndWhile` |
| `loop: ... break` | `\While{\textbf{true}}` with an explicit `\textbf{break}` state, or inline as repeated `\State` steps if that matches how similar loops already read in main.tex — check how the existing `wait until ... continue` loops in `lock`/`upgrade`/`prepare` are rendered and mirror that pattern for new loops |
| `return X` | `\State \textbf{return} X` |
| `X, Y = foo()` / multiple return values | `\State \textbf{return} X, Y` |
| function call `foo(a, b)` | `\Call{foo}{a, b}` |
| `//` or `# comment` | `\Comment{comment text}` at end of the relevant `\State`, or its own `\State \Comment{...}` line for a standalone comment |
| `atomic(X) { ... }` | `\State \textbf{atomic}(X) \{` then indented `\State \indent ...` lines, closing `\}` — exactly as the existing `lock`/`commit`/`abort_transaction` procedures already do |
| `wait until COND` | `\State \textbf{wait until} COND` |
| `CAS(a, b, c)` | `\Call{CAS}{a, b, c}` |
| set literal `{a, b}` | `\{a, b\}` (braces must be escaped in LaTeX) |
| struct literal `Type{f1=v1, f2=v2}` | `\text{Type}\{f1=v1, f2=v2\}` |
| `∈` | `\in` |
| `→` / `->` (mapping) | `\to` |
| `⊇` | `\supseteq` |
| `≠` | `\neq` |
| `∀` | `\forall` |
| `∃` | `\exists` |
| `_` inside identifiers (e.g. `txn_id`) | keep literal in math mode (`$txn\_id$` or write as `\text{txn\_id}$`) — **never** leave a bare unescaped `_` outside math mode, it breaks the LaTeX build |
| `NULL`, status names (`aborted`, `must_abort`, ...) | `\text{must\_abort}` etc., matching how existing statuses like `\text{executed}` are rendered in main.tex |
| procedure/data-structure order | keep the same top-to-bottom order as pseudo.txt within each Part, and keep new procedures in the same Part as their pseudo.txt neighbors (Part 1 = data structures + execute/get/put, Part 2 = lock/upgrade, Part 3 = prepare/commit/abort/abort_transaction/mark_must_abort) |

## Handling structural changes

- **New field on a data structure** (e.g. `txn` gaining `write_buffer`): update the `\State`
  line defining that structure in Part 1's Data Structures block.
- **New status value**: update the `status \in \{...\}` line.
- **New procedure** (e.g. `mark_must_abort`): add a new `\Procedure`...`\EndProcedure` block in
  the Part matching its role, preceded by `\Statex` for spacing, consistent with how procedures
  are already separated in main.tex.
- **Renamed procedure or parameter** (e.g. `abort(txn)` -> `abort(txn_id)` with a `transactions_map`
  lookup added): propagate the rename/lookup into the corresponding `\Procedure` header and body,
  and into every `\Call{...}` site elsewhere in main.tex that invokes it.
- **A TODO/comment added or removed** in pseudo.txt: mirror it as a `\Comment{...}` addition or
  removal — don't invent new commentary that isn't in pseudo.txt.
- If pseudo.txt's structure diverges enough from main.tex's current Part split that a clean 1:1
  transcription isn't obvious (e.g. a genuinely new concern that doesn't fit Parts 1-3), stop and
  ask the user how they want it organized rather than guessing a new structure.

## Overleaf/PDF compatibility checklist (before finishing)

- Every `{` has a matching `}` — count braces in any block you touched, especially nested
  `\text{...}` and set-literal `\{...\}`.
- Every `\Procedure{...}` has a matching `\EndProcedure`; same for `\If`/`\EndIf`, `\For`/`\EndFor`,
  `\While`/`\EndWhile`.
- No bare `_`, `&`, `%`, `#`, `$` outside math mode or `\text{}` — these are special characters in
  LaTeX and will fail to compile or silently mis-render on Overleaf.
- Special Unicode symbols from pseudo.txt (∈, →, ⊇, ≠, ∀, ∃, …) are converted to their LaTeX macro
  equivalents, not pasted verbatim — `algpseudocode` does not reliably render raw Unicode in every
  Overleaf compiler configuration (pdfLaTeX vs. XeLaTeX/LuaLaTeX).
- Each `\algorithmic` block still compiles as one coherent algorithm (no stray `\EndProcedure` etc.
  outside its matching `\Procedure`).
- If you're unsure whether a change compiles cleanly and can't run `pdflatex` locally, say so
  explicitly in your report rather than asserting it renders correctly — don't claim you verified
  PDF output you didn't actually build.

## Automation note

A `PostToolUse` hook on `Edit`/`Write` targeting `pseudo.txt` (configured in
`.claude/settings.json`) automatically injects a reminder to run this skill after every edit to
`pseudo.txt` made through Claude Code. It does not fire for edits made outside Claude Code (e.g.
directly in an editor) — if the user mentions editing pseudo.txt another way, treat that the same
as a hook firing and run this sync.
