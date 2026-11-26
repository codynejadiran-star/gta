# Halcyon Heat — self-hosted with multiplayer

This folder is the whole game plus its own multiplayer server. No packages to install.

```
server.js        the server: serves the game, runs multiplayer (WebSocket /ws) and stores saves
package.json     tells Render how to start it
render.yaml      optional one-click Render setup
public/          the game (index.html, js/ ... and mp-bridge.js)
```

## Put it on Render
1. Make a new GitHub repository and upload everything in this folder (keep the folders as they are).
2. On render.com: **New → Web Service**, connect the repository.
3. Settings: Runtime **Node**, Build command `echo done` (or leave empty), Start command `node server.js`, Instance type **Free** is fine.
4. Create the service. When it says **Live**, open the `.onrender.com` link. Everyone who opens that link is in the same city.

## Good to know
- Free Render services go to sleep when nobody has played for a while; the first visit after that takes up to a minute to wake up.
- Saves and the leaderboard are kept in `data/db.json` on the server. On the free plan that file is wiped when the service restarts or redeploys,
  but every player's progress is also saved in their own browser, so they carry on from where they were.
  To keep server saves forever, add a Render **Disk** (paid) mounted at `/opt/render/project/src/data`.
- Players are anonymous: each browser gets a random id. Names shown to others are the in-game character names.
- To test on your own computer: install Node, run `node server.js` in this folder, open http://localhost:3000 in two browser windows.

## Updating the game later
Run `export` in the game's admin console on claude.ai again, and replace the `public/` folder in your repository with the new one.
Render redeploys automatically when the repository changes.
