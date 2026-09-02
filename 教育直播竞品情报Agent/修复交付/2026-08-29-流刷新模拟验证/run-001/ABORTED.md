# Aborted fixture run

The first accelerated run was stopped after the local HTTP fixture failed to advance when FFmpeg intentionally closed at the 900-second refresh boundary. This was a simulation-server state bug, not a production recorder result. No `simulation-summary.json` was produced; this run is not acceptance evidence.
