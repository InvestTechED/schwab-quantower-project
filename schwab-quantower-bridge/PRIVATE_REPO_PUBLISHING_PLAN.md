## Private Repo Publishing Plan

This project should be published to GitHub as a **private repository first**.

### Objective

Show real engineering work to recruiters without giving away the production source code, operational details, or reusable IP.

### Recommended Repo Strategy

1. Create a **private GitHub repository** owned by you.
2. Push the current stable baseline there.
3. Keep the source private while building a visible GitHub profile.
4. Later, create a separate **public showcase repo** if you want public-facing portfolio material.

### Why Private First

- protects the custom bridge logic
- protects your production workflow and environment assumptions
- avoids accidentally publishing secrets or sensitive paths
- still lets you demonstrate active GitHub usage on LinkedIn and resumes

### What Recruiters Can Still See

Even with a private repo, recruiters can still benefit from:

- your GitHub profile activity
- commit history on repos they are invited to review
- your project descriptions on GitHub profile, LinkedIn, and resume
- screenshots, architecture summaries, and sanitized writeups

### Safe Publishing Checklist

Before pushing to any remote:

1. Confirm `.env` is ignored.
2. Confirm `tokens/*.json` is ignored.
3. Confirm build output, logs, and local caches are ignored.
4. Confirm no production-only paths or secrets are embedded in README examples unless intentionally documented.
5. Prefer screenshots and architecture notes over raw sensitive code excerpts in public-facing materials.

### Public Showcase Option Later

If you want a public portfolio later, create a second repo with:

- screenshots
- architecture overview
- problem statement
- lessons learned
- sanitized snippets only
- no secrets
- no production tokens
- no full live deployment instructions

### Recommended Positioning

Treat this repo as:

- **private production repo** for real source

Treat a future showcase repo as:

- **public portfolio repo** for career visibility

### Suggested Next Step

When ready:

1. create a new private GitHub repo under your account
2. update the local `origin` remote to that repo
3. push the current stable baseline branch
4. add a polished README and project summary

