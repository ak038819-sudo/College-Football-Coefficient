# Elocast design decision: The Modern is the shared team-page system

**Chosen design:** concept **#2 — The Modern**, the dark navy, atmospheric,
data-rich profile. A review group preferred this concept. Extend its style
to existing **team landing pages**, not only the new Matchup Intelligence
feature.

## Scope decision

Create **one visual system** that can power:
- Team Overview: logo/wordmark, available Elo/Merits summaries, current
  roster/record/schedule links and a compact analytics preview.
- Team Offense: opponent-adjusted offensive traits, observed vs adjusted
  values, per-unit schedule difficulty and denominator/source labels.
- Team Defense: defensive percentile directions corrected for
  *lower allowed values = stronger defense*.
- Matchup Intelligence: two opposing units side by side with descriptive
  evidence, **not** a projected score or game-outcome probability.

The established four primary destinations remain
**Home · Stats · Standings · Teams**. This concept is a nested **team-page**
style, not a new top-level navigation system. Preserve team hashes and links.

## Design system

- Deep midnight navy background; tonal sapphire cards and restrained cool
  blue highlights. The home team's accent may color limited details but
  must not reduce contrast or recolor the whole analytical scale.
- Mountain/sports imagery is a *hero accent*, not where interactive
  information or small text is placed. Respect previously supplied team
  graphic panels and existing team brand assets.
- Collegiate display typography for the team identity; highly legible
  tabular numerals for statistics; metadata must remain legible on mobile.
- The radar chart is an *overview*, not the only way to read the metrics.
  Always pair it with accessible exact-value rows, and support raw-versus-
  adjusted comparisons in compact tables/bars.
- Cards: Team Profile, Unit Schedule Difficulty, Sample & Reliability,
  Matchup Intelligence. Reuse structural primitives across all tabs.
- Smaller screens: stack panels, keep team identity and key facts visible,
  render a compact numeric alternative to the radar and preserve touch
  targets. Reduced-motion support and semantic controls required.

## Data binding contract

Consume the validated **`elocast-descriptive-v1`** export from
`src/export_matchup_descriptive.py`. Display `as_of_exclusive` date,
metric provenance, eligible FBS games, distinct opponents, reliability
tier and unavailable/null states correctly.

**Do not copy invented mockup figures into production.** The image study's
Elo/Merits and percentiles are illustrative. The isolated HTML prototype
**deliberately uses dashes** instead of fake numbers.

Avoid changing the underlying Elo/Merits data or formula. Reuse the current
site's team identity, logo, navigation hashes and panel assets.

## Prototype and implementation sequence

Initial isolated prototype:
[`ui/prototypes/modern-team-profile.html`](../ui/prototypes/modern-team-profile.html).

1. Review responsive visual hierarchy and interaction styling; ensure
   coherence with present Home, Stats and Standings UI.
2. Create shared, accessible team-header / tab / trait / schedule / sample
   components within the *existing* dashboard architecture.
3. Wire real descriptive JSON through the existing static-export workflow
   (not a live API). Guard against null values and incomplete seasons.
4. Test desktop, phone, light/dark compatibility, hash navigation, browser
   load, archive years, and the original team pages' existing roster/stats/
   coaches/stadium links.
5. After review, separately authorize deployment and merge.

**Not yet implemented:** integration into actual team routes, real ratings
in the standalone prototype, visual regression approval, or public release.
PR #161 remains draft until separately approved.
