# UCM-52679 AC6 — Seasonal Surcharge Overrides for Contracted Accounts (Quote-UI path)

Captured, reproducible OmniStudio configuration for AC6: the Quote-UI OmniScript
path ("Opportunity with multiple Accounts" → `Pricing_Procedure` Integration
Procedure → `create_qouteandorder` Integration Procedure) now resolves the same
override-vs-standard seasonal surcharge logic as the headless pricing Flow,
verified end-to-end in MCDEV8451 including persisted `Opportunity_Split__c`
totals for both single-account (100%) and multi-account (60/40) splits.

## Why this format, not sfdx source

`OmniProcess`, `OmniProcessElement`, `OmniDataTransform`, and
`OmniDataTransformItem` are standard Salesforce *data* objects in this org's
OmniStudio install, not Metadata API component types — `sf project
retrieve/deploy` does not apply to them, and the Salesforce CLI is not
installed in this environment regardless. `export_omnistudio_config.py` /
`apply_omnistudio_config.py` are this change's deploy mechanism, following
the same `auth.py`-based Python pattern already used for every other
cross-org promotion in this repo.

## Components in scope

### Apex (sfdx source, deployable via Metadata API)
- `force-app/main/default/classes/SL_SeasonalSurchargeResolver.cls` — Remote
  Action (`System.Callable`) wired into the `MultipleAccounts` OmniProcess.
  Includes two fixes discovered only through live OmniScript invocation and
  not reproducible from the headless pricing Flow path:
  - `normalizeLines`: OmniStudio's Loop Block output collapses to a bare
    `Map` (not a single-element `List`) when a repeating block produces
    exactly one iteration.
  - `parseFlightDate`: OmniStudio IP/DataRaptor inputs deserialize dates as
    plain Strings, not Apex `DateTime` — a direct cast throws
    `System.TypeException` at runtime.
- `force-app/main/default/classes/SL_SeasonalSurchargeResolverTest.cls` —
  includes `testCall_singleMapShapedLine_normalizedToList`,
  `testNormalizeLines_listMapNullAndUnsupportedShape`, and
  `testParseFlightDate_stringAndNullAndDateTimeShapes` covering the two
  fixes above.

### OmniStudio configuration (captured in `mcdev8451_captured_config.json`)

**`Pricing_Procedure.MultipleAccounts` Integration Procedure** (active version
at capture time: v9, Id `0jNWK0000008Wif2AE` in MCDEV8451 — Ids are
environment-specific and not part of the portable artifact):
- `SetFinalSurchargePricing` (Set Values, child of `AccountLoop_FinalPricing`)
  — computes `FinalNetUnitPrice`, `FinalNetTotalPrice`, `FinalQuantityyy`,
  `FinalSplitAmount` from `MergeSurchargeResults` using the verified-reliable
  `offerPrice` field (NOT `netTotalPrice`, which returns unreliable
  stub-like values from the resolver's current implementation).
- `AccountLoop_FinalPricing` (Loop Block) — `loopOutput` threads
  `Quantityyy`/`SplitAmount`/`SplitAmtRound`/`QuantityRound` from
  `SetFinalSurchargePricing` instead of relying on DataRaptor Transform
  formula items (which cannot correctly compute into nested list items —
  see "Known platform limitations" below).
- `AccountLoop_WithKey` (Loop Block) — `loopOutput.selectedproductid` fixed
  to read the parent-level `%Transformedproductslist:selectedproductid%`
  instead of the nonexistent `%Transformedproductslist:BillableAccount:selectedproductid%`.
- `MergeSurchargeResults` (List Merge Action) — unchanged join logic,
  `advancedMergeMap` keyed on `lineKey = CONCAT(selectedproductid, ":", selectedAccountid)`.
- `SetBillableAccountLineKey` (Set Values) — unchanged; reference pattern for
  the `AccountLoop_WithKey` fix above.
- `DMTAccounttransforminIP` (DataRaptor Transform Action element) —
  `failOnStepError` reverted to `false` (was left `true` from a diagnostic
  toggle).
- `Response` / `ProductLoop_Surcharge` — all `Debug*` diagnostic
  output/loop fields removed.

**`create_qouteandorder` Integration Procedure** (active version at capture
time: v8, Id `0jNWK0000006Ijp2AE` in MCDEV8451):
- No `OmniProcessElement` changes — the fix here was entirely in the
  `DMTfinalproductlisttransformforOLI` DataRaptor Transform's item mappings
  (below).

**`DMTfinalproductlisttransformforOLI` DataRaptor Transform** — 8 enabled
mapping items corrected from flat `ProductAccountInfo:*` input paths to the
actual nested `ProductAccountInfo:BillableAccount:*` shape produced by
`DMTAccounttransforminIP` (`committedquantity`, `selectedproductid`,
`PricebookentryId`, `selectedAccountid`, `Percentage`, `SplitAmount`,
`ContactId`, `NetUnitPrice` outputs). This DataRaptor's `IsActive` flag is
`false` in MCDEV8451 at capture time because OmniStudio DataRaptor Transform
versions are activated independently per-org and this one hadn't been
explicitly reactivated after the mapping edits in this sandbox; functionally
it is still invoked and resolves correctly by Id reference from the IP (same
pattern as `DMCreateOpportunitySplits` below). Confirm/activate the specific
version during promotion if the target org enforces active-version lookup by
name rather than by stored Id.

**`DMTAccounttransforminIP` DataRaptor Transform** — formula-type items
(`Quantityyy`, `SplitAmount`, `SplitAmtRound`, `QuantityRound`,
`SplitwithPercentage`, `NTP`) remain present but several are `IsDisabled`
— the real computation was moved upstream into the IP's own
`SetFinalSurchargePricing` Set Values step (see above) because DataRaptor
Transform formula items cannot correctly compute into nested list items
(see "Known platform limitations"). The still-enabled non-formula field
mappings (`selectedAccountid`, `selectedproductid`, `NetUnitPriceNumber`,
`QuantityRound`, `SplitAmtRound`, `SplitAmount`, `Percentage`→`SplitPercentage`,
`ContactId`, `PricebookEntryId`, etc.) are unchanged and correct.

**`DMCreateOpportunitySplits` DataRaptor Post Action** — no mapping changes;
confirmed all mappings enabled with correct paths (this was verified, not
modified, during debugging).

### Permanent vs. reverted-debug changes

| Change | Status |
|---|---|
| `AccountLoop_WithKey.loopOutput.selectedproductid` path fix | Permanent |
| `DMTfinalproductlisttransformforOLI` 8 field-path fixes | Permanent |
| `SetFinalSurchargePricing` / `AccountLoop_FinalPricing` surcharge-math fix | Permanent |
| `SL_SeasonalSurchargeResolver` `normalizeLines` / `parseFlightDate` fixes | Permanent |
| `DMTAccounttransforminIP.failOnStepError = true` | Reverted to `false` |
| `Response.additionalOutput` `Debug*` fields | Removed |
| `ProductLoop_Surcharge.loopOutput` `Debug*` fields | Removed |

No `Debug*`-named field remains anywhere in the captured configuration
(verified by text search across the full exported JSON).

### Known platform limitations discovered during this work

1. DataRaptor Transform Formula-type `OmniDataTransformItem`s cannot
   correctly compute values that reference fields inside a nested list
   (e.g. `%Transformedproductslist:BillableAccount:Quantity%` where
   `BillableAccount` is itself a list) — they silently produce `0`/blank
   for every nested item instead of raising an error.
2. An Integration Procedure Set Values step's `elementValueMap` cannot
   reference another key defined in the same step/evaluation pass — doing
   so does not resolve and must be worked around by inlining the full
   source expression.
3. `RA_SeasonalSurcharge`'s `netTotalPrice` response field is not reliable
   for real business totals (observed test-stub-like values such as `0.02`
   unrelated to actual pricing) — `offerPrice` is the reliable final
   adjusted per-unit price and must be used instead for any persisted
   dollar-amount math.

## Files

- `export_omnistudio_config.py` — exports the current state of both
  Integration Procedures and all three DataRaptor Transforms from a given
  org into a portable JSON artifact keyed by business names, not record Ids.
- `apply_omnistudio_config.py` — applies a captured JSON artifact to a
  target org: locates each `OmniProcessElement`/`OmniDataTransformItem` by
  parent-chain + name/field-path, deactivates the owning `OmniProcess`
  before updating its elements (OmniStudio requires this), updates
  `PropertySetConfig` / item fields to match, then reactivates.
- `mcdev8451_captured_config.json` — the verified-working MCDEV8451 state,
  captured after the final regression pass in this document's commit.

## Deployment / promotion steps (MCDEV → target org)

1. Deploy `SL_SeasonalSurchargeResolver.cls` /
   `SL_SeasonalSurchargeResolverTest.cls` via the normal Metadata API
   deploy path (`mcp__bst-ai-os-salesforce__salesforce_deploy` or
   equivalent), running `SL_SeasonalSurchargeResolverTest`.
2. Run `apply_omnistudio_config.py <target_env> mcdev8451_captured_config.json`
   to apply the captured OmniStudio configuration. This script deactivates
   each Integration Procedure before editing its elements and reactivates
   it afterward — both target-org Integration Procedures must already
   exist (same `UniqueName` prefix / `Type`+`SubType`) prior to running
   this, since the script updates existing elements by name match rather
   than creating new Integration Procedures from scratch.
3. No explicit "compile" step is required for Integration Procedure or
   DataRaptor Transform element edits — unlike an OmniScript, saving an
   element's `PropertySetConfig` takes effect immediately once the owning
   `OmniProcess` is reactivated. Confirm the owning `OmniProcess.IsActive`
   is `true` after the script finishes (the script already verifies this
   in its `finally` block, but re-confirm live post-deploy as a second
   check).
