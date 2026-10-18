# Screenshots

Images and clips used by the [README](../../README.md). Record them from the real app with
made-up demo data: no real hostnames, IPs, emails, customer names or keys.

- **Stills:** PNG. **Clips:** GIF, 10-20 seconds, under 10 MB (GitHub doesn't play videos from
  the repo in a README).
- **Size:** the same window size for every shot (about 1440 x 900), captured at 2x. The README
  shows them 800-900 px wide.
- **Theme:** pick one (dark or light) and use it for every shot, so they look like one set.
- **Tools:** the macOS screenshot tool (⇧⌘5) for stills; Kap or CleanShot X for GIFs.

| File             | Where in the README      | What to show                                                                                                                                  |
| ---------------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `dashboard.png`  | Top of the page          | The dashboard Overview with traffic flowing: messages per second in and out, backlog, the throughput chart and a queue table with 3-4 queues. |
| `quickstart.gif` | Quick start              | A terminal: `berthq start`, then in a second pane `queue create`, `publish` and `consume`. Big font, about 15 seconds.                        |
| `consumers.png`  | Consumers and replay     | A queue's Consumers tab with 2-3 consumers (one behind), and the replay dialog open.                                                          |
| `messages.png`   | Dashboard and monitoring | A queue's Messages tab with one message open in the body viewer.                                                                              |

For `quickstart.gif`, a terminal recorder makes a sharper clip than a screen recording, for
example [VHS](https://github.com/charmbracelet/vhs) or asciinema with `agg`.

## Social preview

`social-preview.png`, 1280 x 640, PNG under 1 MB. It isn't used in the README: upload it in
GitHub under **Settings → General → Social preview**. It is the image people see when the link
is shared on X, Slack, Reddit or LinkedIn. Show the icon, "Berthq", the tagline from the top of
the README and a cropped screenshot.
