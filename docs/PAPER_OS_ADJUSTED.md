# Additional tag-aware OS paper figures

These optional variants split two existing vendor groups using the already
stored resolved `OS_TAG` decisions. They do not change the production OS
taxonomy, existing reports, classification tables, figures or numeric keys.
No new OS scan, IP-ID measurement or IP-ID classification is performed.

| Original family | Adjusted group | Display label | Membership |
| --- | --- | --- | --- |
| `mikrotik` | `mikrotik-routeros` | MikroTik RouterOS | only `OS_TAG=mikrotik-routeros` |
| `mikrotik` | `mikrotik-swos` | MikroTik SwOS | every other resolved tag previously mapped to `mikrotik` |
| `hpe-network` | `hpe-comware` | HPE Comware | only `OS_TAG=hpe-comware` |
| `hpe-network` | `hpe-network` | HPE Network OS | every other resolved tag previously mapped to `hpe-network` |

Case/whitespace normalization reuses the existing taxonomy mapper. In particular,
the requested SwOS remainder also contains the generic resolved tag `mikrotik`
if present; the display name does not imply a separate positive SwOS fingerprint.
Unknown tags still fail rather than being inferred from a string prefix.
All other groups retain their original membership and colors. New companion
shades distinguish the split subgroups.

## One-time preparation and ordinary rendering

Run on the Analysis VM with its existing mounted inputs, bounded RAM scratch
and appropriate DuckDB thread/memory limits. From the updated repository:

```sh
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--os-adjusted --dry-run"

# Explicit one-off preparation; unchanged caches are reused.
make paper-plots CAMPAIGN_ID=geo-full-20261107-r1 \
  ARGS="--os-adjusted --prepare-os-adjusted --workers 4"

make paper-values CAMPAIGN_ID=geo-full-20261107-r1 ARGS="--strict --timings"
make paper-figures-zip
```

Subsequent render-only runs use `ARGS="--os-adjusted --workers 4"`. The prepare
flag is optional once the compact aggregates exist. Both modes leave existing
PDFs untouched; they are not overwrite/refresh commands. Default `paper-plots`
runs do not discover or prepare the adjusted variants implicitly. Do not combine
`--os-adjusted` with `--final-only` or `--validation`.

Preparation is sequential and may take materially longer than a design-only
render: the old grouped aggregates no longer retain tag distinctions. The
shared resolved OS tag map is written once per OS input into a separate
`data/processed/paper-adjusted-tags/` cache. Existing measured strategy labels
are then joined to that map. Only existing labels and OS evidence are read;
bulk raw IP-ID sequences are never opened. For ITDK bars, the existing
ITDK/OS role join supplies the original exact-IP `T` decisions.

A new compact `*_operating-system-group-by-strategy__adjusted.json` is published
beside each original processed aggregate. It contains exact counts, newly
normalized rows, role counts and input fingerprints. It does not replace the
original JSON/PQ or OS-group cache. Normal render-only runs read these small
caches and check input metadata; they do not issue per-IP queries. Changes to
the adjusted renderer's layout do not invalidate the numerical preparation.
Missing, stale or incompatible preparation fails explicitly without falling back
to combined vendor counts. Existing stale caches are never silently replaced.

Preparation checks that the new subgroups sum to their original vendor groups,
all measured/resolved totals are conserved, and unchanged families retain their
counts. Where the original small aggregate is available, every original
OS-group/strategy cell must be reproduced after folding the split groups back.
Raw/grouped OS IPs must be unique. Existing Router/non-transit group counts are
also conserved before applying the Final complement definition.

## Outputs, populations and labels

The suffix is appended to the selected paper variant:

| Population/family | Additional PDF |
| --- | --- |
| Stateless merged OS-group heatmap | `n-rt-b_fi-m_operating-system-group-by-strategy__paper__adjusted.pdf` |
| ITDK Final OS bars | `n-rt-b_fi-m_itdk-role-by-os__paper_final__adjusted.pdf` |
| Pinned TCP-Connection Full RT-BASE heatmap | `c-rt-b_operating-system-group-by-strategy__paper__full__adjusted.pdf` |

With all six VPs available, there are 18 merged heatmaps, 18 Final OS bar plots
and six Full heatmaps. Discovery respects the registered Full campaign and
completion inventory. Normal reports reuse the same canonical-run selection as
`paper-values`, without depending on existing source PDFs. Historical intermediate
run directories and connection-sample heatmaps are not adjusted
or substituted for Full measurements.

Heatmap rows are normalized by the newly split group's measured, resolved
population. Both children of each split have their own rows and inline IP
counts. The heatmap y-label is exactly
`Operating-System Group / Vendor (#IP Addr.)`.

The ITDK bars retain the Final populations: Router is the unchanged exact-IP
`T=true` set; All Other is the entire measured population minus Router.
Both bars additionally require resolved OS evidence. Each bar normalizes
independently. The existing top-nine-plus-Other group selection is applied
to the new groups, so small groups may be combined into the display's Other
segment; complete counts remain in the adjusted cache.
The bar y-label remains `Device Type`, and its x-label becomes exactly
`Operating-System Group / Vendor [%]`.

Linux Libertine 7 pt, 3.33-inch width, fixed 10-pt heatmap row heights and
pastel colors are reused. Bar legends retain aligned column pairs, 4-pt squares,
1.2-pt label gaps and automatic column gaps capped at 4 pt. Longer adjusted
labels use fewer legend columns/additional rows when necessary, keeping the
font size, physical bar geometry and page width unchanged.

## Figure exports

After rendering, ordinary `paper-values` prefers the adjusted variants for the
existing figure keys, before Final/paper/original variants in the same canonical
run. The pinned Full figure keeps its `.full` key. Adjusted PDF receipts are
bound to the corresponding original source report; an invalid preferred
variant fails strict export. The original Full completion inventory is read
only and is not rewritten. `paper-figures-zip` includes the selected adjusted
PDFs automatically with the original directory structure.

Historical numeric OS keys still describe their existing combined groups.
This change does not silently reinterpret them as RouterOS/Comware values or
edit the manuscript. Use the adjusted aggregate for any later numeric export
that needs the new groups.
