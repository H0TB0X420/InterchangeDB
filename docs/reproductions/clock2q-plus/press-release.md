# Press release — working-backwards brief for the Clock2Q+ writeup

The blog post gets written against this document. Public-facing: no
internal planning vocabulary. Scope: the reproduction + the native-trace
experiment. The engine improvements found along the way are a separate
post.

---

## 1. Headline — the post in one line

**The algorithm survived replication. Its benchmark didn't.**

(Alternate: *We rebuilt a VMware cache paper from scratch and confirmed
its headline number — then pointed a real database at it, and the
leaderboard flipped.*)

## 2. What's in it — the shape of the finished thing

1. **Cold open (the flip).** One algorithm finished dead last across
   106 industry traces — worse than policies from 1993. On a real
   database's page stream, the same algorithm won. Nothing about the
   algorithms changed. Only the benchmark did.
2. **The paper, in one metaphor.** Clock2Q+ guards a cache with a
   velvet rope: a block that gets re-touched in the seconds after it
   arrives isn't "popular," it's one B-tree page serving one burst of
   adjacent reads — like counting the same person re-entering a club
   after a smoke break as three fans. The paper's one idea is a
   *correlation window* that refuses to count those re-entries.
3. **The reproduction.** Rebuilt from the paper's prose alone — every
   underspecified decision logged and numbered (15+ ambiguities, three
   worth telling the authors about). A reference implementation, we
   later discovered, had quietly landed in the authors' simulator two
   months before we started; we diffed against it only AFTER our
   results were in. The blind reimplementation matched their code
   exactly on the paper's core mechanism (window boundary, orientation,
   and arithmetic — three decisions the prose does not pin down), and
   the diff surfaced a discrepancy in the authors' own tree: the code's
   ghost defaults to 90% of cache where the paper says 50%. Then their
   own dataset: 106 CloudPhysics traces, 2.1 billion requests, their
   exact derivation, their exact cache sizes.
4. **The verdict table.** Claimed: up to 28.5% fewer misses than its
   strongest rival at large caches. Measured: up to 36.9%. Best
   policy on 71 of 106 traces at the largest size. The internal
   mechanism verified by a figure-free instrument: promotion counts
   drop 20–34×, exactly the paper's Table 1 signature. Replication:
   clean.
5. **The twist.** The paper never had real metadata traces — it
   *manufactured* them, dividing disk addresses by 200. We have an
   actual B-tree. Capture 4.8 million real page requests from a live
   TPC-C run, rerun the identical tournament: the 1993 algorithm
   (LRU-K) goes from median −71% to **winning at +13%**, ARC takes the
   small caches, and Clock2Q+'s signature advantage shrinks below one
   point. Real B-trees have a persistently hot upper tree — frequency
   structure the manufactured traces simply don't contain.
6. **What this means.** Not "the paper is wrong" — the paper is
   *right on its benchmark*, and we can prove it. The finding is that
   the benchmark-manufacturing method, used across this literature,
   doesn't represent at least one real engine's traffic. A replication
   that ends in a confirmation AND a boundary is worth more than
   either alone.
7. **The 1993 footnote (verified against the SIGMOD '93 text).** The
   algorithm that flipped — LRU-K — comes from a 1993 paper that
   *anticipated the 2025 paper's problem, by name*. O'Neil et al.
   §2.1.1 defines the "Correlated Reference Period": re-accesses
   within the period are collapsed — "*if a reference to a page p is
   made several times during a Correlated Reference Period, we do not
   want to penalize or credit the page for that*" — they update a
   last-touched clock but never the K-distance history, and pages
   inside the period are ineligible for eviction (protected probation:
   S3-FIFO's Small queue, in 1993). Their motivating example is
   *B-tree leaf pages under concurrent transactions* — the exact
   workload of both our experiment and the 2025 paper. And O'Neil says
   the idea wasn't even his: he credits Robinson & Devarakonda, 1990.
   Modern reimplementations — ours included, like the teaching
   versions most engineers learn from — drop the clause. Clock2Q+'s
   correlation window is the 1990→1993 idea rediscovered in FIFO-queue
   form, and our two result tables are the two halves of the
   demonstration: on real B-tree traffic the missing clause doesn't
   matter (genuine frequency signal — LRU-K wins anyway); on the
   manufactured traces, omitting it is fatal (burst-dominated signal —
   LRU-K finishes last). Thirty-five years between a footnote and its
   vindication.
8. **Receipts.** Repo with every trace checksummed, every CSV
   published, the ambiguity log, and a note on what we sent the
   authors.

## 3. Problem it solves for the reader — why anyone clicks

Every engineer consumes systems papers on faith. Almost nobody checks
them — checking is assumed to require the authors' code, their
harness, their data. This post shows the full anatomy of a check that
required none of those: rebuild from prose, reproduce the number,
*then* ask the one question the authors structurally couldn't — does it
survive contact with a real system? The reader leaves with a
transferable template: confirm the claim, then attack the benchmark,
and knowing that "reproduced" and "generalizes" are different claims.

## 4. What Phil or a hiring manager should say after

> "He didn't just read the paper — he rebuilt it faithfully enough to
> land within a few points of the headline number, logged every
> judgment call where the prose ran out, and then designed the one
> experiment the authors couldn't run. And when his result disagreed
> with theirs, he didn't dunk — he found the precise boundary where
> their claim stops holding and reported both sides with receipts.
> That's the person I want deciding whether we adopt things."

## 5. The experiment — built, measured, failure modes

**Built:** four cache policies from the paper's prose (Clock2Q+ and its
whole design lineage: S3-FIFO 1-bit/2-bit, Clock2Q) on our database's
swappable eviction-policy interface; a trace simulator speaking the
CloudPhysics formats; the paper's trace-derivation transform; and a
page-request recorder inside our buffer pool (bounded, off the hot
path, policy-independent by construction — it logs what the B-tree
*asks for*, not what happens to be cached).

**Measured:** miss ratio for ten policies at the paper's four cache
sizes (0.5%–10% of each trace's footprint) — across all 106 CloudPhysics
traces (2.1B requests), then across 4.79M captured requests from a live
TPC-C run. Plus the paper's Improvement-vs-Clock metric and its Table-1
movement counters (Small→Main / Small→Ghost / Ghost→Main), which
validate the *mechanism* rather than just the outcome.

**Could have failed:** the rebuilt policy might not match the paper's
curves (implementation divergence — the ambiguity log exists because
the prose underdetermines the algorithm in 15+ places); the traces
might have been unobtainable (they nearly were — the canonical archive
is click-through-gated; a public mirror saved the project); or the
native experiment could have returned "ranking transfers unchanged" — a
null result. We'd have published that too; it just wouldn't have made
this headline.

## 6. Not on the box — explicitly out of scope

- **The engine improvements** discovered while running at paper scale
  (several policies' scalability fixes, a 2Q faithfulness bug). Real,
  but a side effect — separate post.
- **Production concerns** the paper covers and we don't reproduce:
  dirty-page handling, lock behavior, live cache resizing (§4/§5.5).
- **The other six baselines** in the paper's evaluation (LIRS,
  Clock-Pro, TinyLFU, etc.) — stock simulator implementations there;
  we compare against the paper's reported curves, not reimplementations.
- **Sensitivity sweeps** (window size, ghost size, hand-movement limit)
  — reproduction targets, not part of this story.
- **Generality of the native result:** one workload (TPC-C), one
  configuration, a footprint 270× smaller than a CloudPhysics trace.
  The claim is "does not transfer to at least one real engine," never
  "does not transfer."
- **Any verdict on the authors' integrity** — the paper is right on
  its own benchmark; we say so in the first third of the post.
- **The obvious sequel** (queued, not in this post unless it lands
  first): put O'Neil's 1993 clause BACK into LRU-K, as specified, and
  see whether the 2025 algorithm beats the old literature done right.
  If that flips the derived-trace result, it becomes the next post's
  headline — and this post's footnote becomes its trailer.
