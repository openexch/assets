# Scenario map (RC1 S8, G6 deliverable)

Every scenario id named in the RC1 specs, mapped to the test class and method that covers it, or "not covered" with a
reason. Built by grepping `assets-cluster/src/test` at `182c86c` (S1..S7 GREEN) plus the S8 sizing/restore classes, then
reading the matched methods. "Partial" means the behaviour is pinned but not in the exact shape the spec row names.

Sources: `notebook/reports/runtime-handoff-execution/RC1-core/spec/` lifecycle.md (R-C*, section C), risk.md (C1..C20,
section C), synthetic-marketdata.md (R1..R18), hardening-k5-k6.md (RED-F1..F22), harness.md (B4 names), and
`notebook/plans/runtime-dayaniklilik-2026-09-27/is-paketleri.md` (RC1-A01..A06).

Class paths: `order/` = `com.openexchange.assets.application.engine.order`, `persistence/` =
`com.openexchange.assets.infrastructure.persistence`, `cluster/` = `com.openexchange.assets.cluster`, `determinism/` =
`com.openexchange.assets.determinism`, `time/` = `com.openexchange.assets.application.engine.time`, `auth/` =
`com.openexchange.assets.infrastructure.auth`.

## Lifecycle (lifecycle.md section C)

| id | covered by | note |
|---|---|---|
| R-C1a | order/OrderAdmissionTest.structuralRejectsCreateNothingAndConsumeNoBudget; order/RiskChecksTest.c17_perMarketIsolationAndNoUserStateFromStructuralRejects | unknown market, bad qty/side, FOK, flags, past GTD, missing requestId; raw enum bytes on the wire are decode rejects (persistence/IngressFenceTest.invalidBoolFlagByteIsDecodePoisonNotAThrow) |
| R-C1b | order/RequestIdTest.redC1b_sameRequestSamePayloadReplaysTheDecision | |
| R-C1c | order/RequestIdTest.redC1c_sameRequestDifferentPayloadIsAMismatch | reason is REQUEST_ID_PAYLOAD_MISMATCH |
| R-C1d | order/RiskChecksTest.c1_limitRaisedAfterHistoryIsDecidedNormally | |
| R-C1e | order/OrderAdmissionTest.marketBuyUsesTheIntegerBudgetPriceOrTheClientBudget | reason is NO_REFERENCE_PRICE (not NO_LIQUIDITY), no hold |
| R-C2a | order/OrderAdmissionTest.redC2a_limitBuyWithExactFundsIsAcceptedInOneApply | |
| R-C2b | order/OrderAdmissionTest.redC2b_anActiveClientOrderIdIsRejectedAndReusableAfterTheOrderIsGone | |
| R-C3a | order/SyntheticAdmissionTest.section1_armedStopIsPendingTriggerWithoutDispatchAndTakesASlot; order/SyntheticAdmissionTest.section1_buyStopLossBudgetZeroHoldsStopPriceTimesSlippageTimesQtyExactly | |
| R-C4a | order/OutcomeAndDispatchTest.rC4a_createOutcomeAppliedLearnsTheLegAndAcksNew | |
| R-C4b | order/OutcomeAndDispatchTest.rC4b_staleAttemptOrKindIsIgnoredAndCounted; order/OutcomeAndDispatchTest.rC4b_redeliveryAfterResolutionIsCountedNotReapplied | |
| R-C5a | order/FillAndTerminalTest.rC5a_partialFillSettlesFromTheOrdersHoldAndCreditsTheCounterparty; corpus order_fill_partial_then_filled | |
| R-C5b | order/FillAndTerminalTest.rC5b_sameTradeIdTwiceIsAByteLevelNoOp | |
| R-C5c | order/FillAndTerminalTest.rC5c_overfillFillsToRemainingAndEmitsTheExcessAsAnOrderFault | |
| R-C6a | order/TriggerLifecycleTest.R9_buyStopLimitFilledBelowItsLimitReleasesTheResidual; corpus buy_price_improvement_residual_release | same row as synthetic R9 |
| R-C6b | order/IcebergTest.r10_sliceTwoCrossesBelowTheLimitAndTheParentFillsWithTheResidualReleased; order/IcebergTest.c10_sliceOneExhaustedRefillsSliceTwoInTheFillApply | partial: 3 slices, not the 10-slice / 9-refill count of the row |
| R-C6c | order/FillAndTerminalTest.rC6c_fillCompletingAnOrderWithAnOpenAmendWindowReleasesEverything | |
| R-C7a | order/TerminalAndInvariantsTest.rC7a_terminalCancelledForTheCurrentLegReleasesTheResidualOnce; corpus order_cancel_terminal_release | |
| R-C7b | order/TerminalAndInvariantsTest.rC7b_terminalForALegThatIsNotCurrentIsIgnoredAndCounted | |
| R-C7c | order/IcebergTest.d8_sliceCancelledByTheMeEndsTheParentCancelledWithTheTrueFilled | superseded: S6 contract 3.4 / D8 decided that an ME-cancelled slice ends the parent CANCELLED (not slice bookkeeping) |
| R-C8a | order/TriggerLifecycleTest.C8_cancelArmedStopIsCancelledInTheSameApplyWithoutMeCommand; order/SyntheticAdmissionTest.R14_stopTakesASlotAndItsCancelFreesExactlyOne | |
| R-C8b | order/OutcomeAndDispatchTest.rC8b_cancelWhileCreateUnresolvedIsMintedExactlyOnceAtAck | |
| R-C8c | order/OutcomeAndDispatchTest.rC8c_twoCancelsOnARestingLegProduceOneDispatch | |
| R-C9a | order/OutcomeAndDispatchTest.rC9a_gtdTenSweepsMintOneCancelAndTheCancelOutcomeYieldsExpired (+ rC9a_gtdJournalTerminalCancelledYieldsExpiredAndTheLateOutcomeIsStale, rC9a_gtdExpiryBeforeAckMintsTheCancelAtAckOnly); cluster/SweepSurvivesRestoreClusterTest; cluster/CoreChunkedRestore100kClusterTest.oneHundredThousandOrdersRestoreFromAChunkedSnapshotWithinThirtySeconds; corpus order_gtd_expiry_cancel_once | |
| R-C9b | order/TriggerLifecycleTest.R12_gtdArmedSellStopExpiresInTheSweepAndNeverFiresAfter | |
| R-C9c | time/DeadlineIndexTest.popsDueEntriesInDeadlineIdKindOrderAndOnlyOnce; time/DeadlineIndexTest.theListingIsAFunctionOfTheEntriesNotTheInsertionOrder; persistence/SweepTimerServiceTest.anEmptyIndexArmsNothingAndANonEmptyIndexArmsExactlyOneSweep | partial: no 100k-GTD splice run; CoreSnapshotSizingTest round-trips 2k GTD + 100k request TTLs, the 100k cluster restore rebuilds 100,002 deadlines |
| R-C9d | cluster/SweepSurvivesLeaderChangeClusterTest.theGtdCancelIsMintedExactlyOnceAtTheDeadlineByTheNewLeaderAfterTheOldLeaderIsKilled | |
| R-C10a | order/IcebergTest.c10_currentSliceFilledTerminalWithSliceRemainingFaultsAndRefills; order/IcebergTest.c10_previousSliceFilledTerminalIsACountedNoOp | |
| R-C11a | order/TriggerTest.R1_buyStopLossFiresOnAskAtStopWithMarketChildBudgetEqualToHold | |
| R-C11b | order/TriggerTest.R1_buyStopLossDoesNotFireWhileAskBelowStop; order/TriggerTest.R3_bidStayingAboveSellStopNeverFires | partial: the core tick carries both sides (S6 3.1), so "only bids change" is a tick with an unchanged ask |
| R-C11c | order/TriggerTest.allOrdersFiredByOneTickAreDispatchedInAscendingOrderId | |
| R-C11d | order/TriggerTest.sellTrailingMirrorsOnTheBidAndSkipsAnEmptyBid; order/SyntheticSnapshotTest.G4_armedStopsTrailingArmsAndIcebergSliceStateSurviveTheCodecWithBooksRebuilt | |
| R-C12a | order/AmendTest.rC12a_amendUpWithInsufficientFundsIsRejectedWithoutAWindow | |
| R-C12b | order/AmendTest.rC12b_amendDownAfterAPartialFillThenCancelLeavesNothingLocked; corpus order_amend_resolved | |
| R-C12c | order/AmendTest.rC12c_meRejectsTheAmend_abortReleasesTheDeltaAndKeepsTheOldLeg; corpus order_amend_aborted | |
| R-C12d | order/AmendTest.rC12d_terminalOnTheNewLegBeforeTheOutcomeResolvesThenCancels | |
| R-C12e | order/AmendTest.rC12e_secondAmendWhileOneIsPendingIsRejectedWithoutATopUp | |
| R-C13a | order/OutcomeAndDispatchTest.rC13a_iocRestingPartiallyIsCancelledOnceAndTheResidualReleased; corpus order_ioc_rest_cancel | |
| R-C13b | order/OrderAdmissionTest.structuralRejectsCreateNothingAndConsumeNoBudget (FOK case); order/SyntheticAdmissionTest.section1_iocAndFokOnEverySyntheticTypeAreUnsupportedTif | D-Q4 taken as "reject at admission" (UNSUPPORTED_TIF) |
| R-C15a | order/TriggerLifecycleTest.D8_childRejectedByOutcomeEndsParentRejectedAndReleases; order/TriggerLifecycleTest.G9_journalRejectedForUnlearnedChildLegBeforeTheOutcome | |
| R-C16a | order/CoreSnapshotSizingTest.tenThousandUsersAndOneHundredThousandOrdersMatchTheDeclaredLayoutAndRestoreByteIdentical; cluster/CoreChunkedRestore100kClusterTest.oneHundredThousandOrdersRestoreFromAChunkedSnapshotWithinThirtySeconds; order/AmendEdgeCasesTest.openReplaceWindowSurvivesACodecRoundTrip | partial: the 100k mix is 5k armed stops / 2k GTD / 1k icebergs, no replace windows at scale; "next sweep identical" is not asserted at 100k |

## Risk (risk.md section C)

| id | covered by | note |
|---|---|---|
| C1 | order/RiskChecksTest.c1_limitRaisedAfterHistoryIsDecidedNormally | |
| C2 | order/OrderSnapshotSectionsTest.ordersRiskAndRequestsSurviveRestoreWithIndexesRebuilt; order/OrderSnapshotSectionsTest.theSpliceRunnerHoldsWithOrderVerbs | partial: the window counters ride the splice runner; no explicit "secCount = 7 mid-bucket" pin |
| C3 | not covered | needs a 3-node cluster test (90 orders on L1, kill, 20 on L2); no such test exists |
| C4 | order/RiskChecksTest.c4_rateBudgetIsConsumedByLaterRejectsNotByRateRejects | |
| C5 | order/RiskChecksTest.c5_fixedWindowBoundaryBurst | |
| C6 | order/RiskChecksTest.c6_backwardsClusterTimeDoesNotRollTheBucket | |
| C7 | order/OrderAdmissionTest.breakerBlocksCreateButNotCancel; order/AmendEdgeCasesTest.circuitBreakerRejectsAmendWhileCancelStillPasses; order/SyntheticRiskAndTriggerGapTest.triggerFiresDespiteBreakerGateAndTightenedRiskConfig | |
| C8 | not covered | Q2 (automatic breaker deleted) has no test asserting the schema lacks the fields or that a 50% move trips nothing |
| C9 | order/RiskChecksTest.c9_collarVectorsOnTheLastAppliedTrade | |
| C10 | order/RiskChecksTest.c10_overflowNeverThrows | |
| C11 | order/OrderAdmissionTest.marketBuyUsesTheIntegerBudgetPriceOrTheClientBudget | |
| C12 | order/OrderAdmissionTest.theHoldIsTheBalanceCheck_insufficientFundsLeavesOnlyTheRateCounter | |
| C13 | order/RiskChecksTest.c4_rateBudgetIsConsumedByLaterRejectsNotByRateRejects | partial: named in the RiskChecksTest header; no spy proves `Account.hold` is never called on a rate reject |
| C14 | order/OrderAdmissionTest.slotIsMembershipInTheOpenSet; order/SyntheticAdmissionTest.R14_stopTakesASlotAndItsCancelFreesExactlyOne; order/TerminalAndInvariantsTest.i8_theSlotTakenAtAcceptIsReturnedByTheTerminal | |
| C15 | order/RiskChecksTest.positionLimitWithOverflowGuard | |
| C16 | order/RiskChecksTest.c16_riskConfigStateMachine | |
| C17 | order/RiskChecksTest.c17_perMarketIsolationAndNoUserStateFromStructuralRejects | |
| C18 | order/OrderSnapshotSectionsTest.ordersRiskAndRequestsSurviveRestoreWithIndexesRebuilt; determinism/SnapshotCanonicalOrderTest.snapshotBytesAreAFunctionOfStateNotHistory | partial: canonical-order test is on accounts; users inserted in different hash orders is not pinned for the risk block |
| C19 | order/OrderAdmissionTest.genesisGateRefusesOrdersWithoutAnyUserState; order/RiskChecksTest.c17_perMarketIsolationAndNoUserStateFromStructuralRejects | partial: not the 1000-user count |
| C20 | order/RiskChecksTest.c20_cancelIsFreeOfRateBudget; order/AmendAdmissionTest.amendConsumesTheRateBucketAndCancelDoesNot | |

## Synthetic orders and market data (synthetic-marketdata.md)

| id | covered by | note |
|---|---|---|
| R1 | order/TriggerTest.R1_buyStopLossFiresOnAskAtStopWithMarketChildBudgetEqualToHold; order/TriggerLifecycleTest.R1_childAppliedFilledParentFilledResidualReleased; corpus synth_stop_buy_budget_child | |
| R2 | order/TriggerTest.R2_buyTrailingFollowsLowerAsksThenFiresAtDeltaWithMarketChild; order/TriggerTest.R2_buyTrailingArmingTickNeverFires; corpus synth_trailing_buy_arm_trigger | |
| R3 | order/TriggerTest.R3_bidStayingAboveSellStopNeverFires; corpus synth_stop_sell_trigger_fill | |
| R4 | order/TriggerTest.R4_bidFallingThroughSellStopFires | |
| R5 | not covered as a core test | the core's MarketTick carries both sides by contract (S6 3.1, K4); writing 0 for an untouched side is a forwarder stitching defect (RC2). MARKET BUY with an explicit budget is admitted (OrderAdmissionTest.marketBuyUsesTheIntegerBudgetPriceOrTheClientBudget) |
| R6 | order/TriggerTest.R6_sameOrLowerBookVersionIsDroppedAndAppliesOnce | |
| R7 | order/SyntheticSnapshotTest.R7_theSpliceRunnerHoldsWithSyntheticCreatesAndMarketTicks | |
| R8 | not covered | needs a leader kill between tick apply and forwarder send; RequestPendingDispatch replay is pinned in-process (order/TriggerLifecycleTest.pendingDispatchReplaysTheChildPayloadByteIdentically) |
| R9 | order/TriggerLifecycleTest.R9_buyStopLimitFilledBelowItsLimitReleasesTheResidual | |
| R10 | order/IcebergTest.r10_sliceTwoCrossesBelowTheLimitAndTheParentFillsWithTheResidualReleased; corpus synth_iceberg_three_slices | |
| R11 | order/IcebergTest.r11a_cancelWhileASliceCreateIsPendingIsMintedAtTheSliceAck; order/IcebergTest.r11b_cancelInFlightWhenTheSliceExhaustsEndsTheParentCancelledNow | |
| R12 | order/TriggerLifecycleTest.R12_gtdArmedSellStopExpiresInTheSweepAndNeverFiresAfter; order/SyntheticRiskAndTriggerGapTest.R12_gtdArmedSellTrailingExpiresInTheSweepAndALaterTickDispatchesNothing; order/SyntheticRiskAndTriggerGapTest.R12_gtdArmedBuyTrailingExpiryReleasesTheEstimateHold | |
| R13 | order/SyntheticAdmissionTest.R13_alreadyCrossedSellStopIsRejectedAfterTheRateCheckAndStored; order/SyntheticAdmissionTest.R13_alreadyCrossedBuyStopIsRejectedAfterTheRateCheckAndStored; order/SyntheticAdmissionTest.R13_anEmptyReferenceSideIsNotCrossed | |
| R14 | order/SyntheticAdmissionTest.R14_stopTakesASlotAndItsCancelFreesExactlyOne | |
| R15 | order/OutcomeAndDispatchTest.rC13a_iocRestingPartiallyIsCancelledOnceAndTheResidualReleased; order/FeedEdgeCasesTest.iocFirstTradeBeforeTheCreateOutcomeAcksFillsThenMintsOneCancelOnlyWhenRemaining | FOK half: R-C13b |
| R16 | order/SyntheticAdmissionTest.R16_degenerateShapesAreRejectedInvalid | |
| R17 | not covered | needs the ME epoch handshake (Q6), an RC2 protocol |
| R18 | not covered | load measurement (100k armed stops + 10k trailing per market, 50 ticks/s for 60 s); `armedStopCount`/`armedTrailingCount` exist, no test |

## Hardening (hardening-k5-k6.md)

| id | covered by | note |
|---|---|---|
| RED-F1 | persistence/IngressFenceTest.truncatedFrameCannotReadStaleBackingBytes | |
| RED-F2 | persistence/IngressFenceTest.shortDeclaredBlockCannotHideFullStalePayload | |
| RED-F3 | persistence/IngressFenceTest.wrongSchemaIdIsDroppedBeforeAnyBodyDecode | |
| RED-F4 | persistence/IngressFenceTest.versionOutsideExactSixIsDropped | |
| RED-F5 | persistence/IngressFenceTest.invalidBoolFlagByteIsDecodePoisonNotAThrow | |
| RED-F6 | persistence/IngressFenceTest.poisonInsideASettleBatchDropsTheWholeBatch | |
| RED-F7 | persistence/IngressFenceTest.settleBatchCountBeyondTheFrameIsAFrameBoundsReject | |
| RED-F8 | persistence/IngressFenceTest.settleWithUnknownMarketIsRejectedLoudlyNotThrown; persistence/IngressFenceTest.settleNotionalOverflowAndBadAmountsAreRejectedLoudly | |
| RED-F9 | persistence/IngressFenceTest.outOfRangeAssetIdIsRejectedAtBothLayersAndCreatesNoAccount | |
| RED-F10 | persistence/IngressFenceTest.unknownTemplateIsCountedNotSilent | |
| RED-F11 | persistence/AssetsApplyFailureFenceTest.serviceFailureStopsAdmissionAndSnapshotsImmediately; persistence/IngressFenceTest.poisonInputDoesNotFenceTheHealthyEngine | |
| RED-F12 | persistence/AssetsApplyFailureFenceTest.errorAfterMutationIsAlsoAnApplyFailure | partial: the Error case; IAE and IOOBE after mutation are not separate cases |
| RED-F13 | persistence/AssetsApplyFailureFenceTest.actualApplyFaultUsesHooklessProcessExit | |
| RED-F14 | cluster/SessionEgressQueueOversizeTest.oversizeFrameIsDroppedCountedAndTheQueueKeepsFlowing; cluster/EgressCloseAtCapClusterTest.closesTheResumableSessionAtItsCapAndTheClusterStaysLive | partial: oversize drop is unit-level, not the 32k-term cluster drive |
| RED-F15 | persistence/SnapshotEnvelopeAdoptionTest.aFlippedBitIsRefusedAndStopsTheNode; cluster/CoreChunkedRestore100kClusterTest.aCorruptedSnapshotByteIsRefusedAndTheNodeNeverServes | the cluster method flips a byte in the archived recording (flag `-Dcore.restore.100k=true`) |
| RED-F16 | persistence/SnapshotEnvelopeAdoptionTest.aBarePayloadIsRefusedAndStopsTheNode; determinism/BalanceSnapshotCodecTest.olderLayoutTagsAreRefused | |
| RED-F17 | persistence/SessionAuthorityDemuxerTest.redF17_aTemplateOutsideTheSessionClassIsRejectedNotApplied; persistence/CoreConfigWireTest.gatewayClassSessionCoreConfigIsRefusedUnauthorizedClassAndNothingIsRecorded | |
| RED-F18 | persistence/SessionAuthorityDemuxerTest.redF18_aUserOutsideTheSessionScopeIsRejected | |
| RED-F19 | persistence/SessionAuthorityDemuxerTest.redF19_productionClassesMustRegisterBeforeAnythingButAQuery | |
| RED-F20 | persistence/SessionAuthorityDemuxerTest.redF20_aClassMismatchOrANonResumableRegistrationIsRejectedAndTheSessionClosed | |
| RED-F21 | persistence/SessionAuthorityDemuxerTest.theGateIsAdminOnlyAndItsStateIsBroadcast; order/OrderAdmissionTest.genesisGateRefusesOrdersWithoutAnyUserState; order/OrderAdmissionTest.aMarketGateBlocksOnlyItsMarket | |
| RED-F22 | auth/CoreAuthenticatorTest.unknownKeyWrongSecretMissingCredentialsAndChallengesAreRejected; cluster/AuthenticatedClusterTest.sessionsAreAuthenticatedClassedScopedAndRegistered | |

## Harness B4 names (harness.md B4)

| name | covered by | note |
|---|---|---|
| LifecycleTrafficGenerator + core_lifecycle_production_shape.scenario | not covered at 182c86c | the production-shaped lifecycle generator and its 3k-line corpus file do not exist yet; ledger_churn_production_shape is the money-only predecessor |
| CoreSnapshotReplayEquivalenceTest.restoringAtAnyPointReproducesTheUninterruptedRun | determinism/SnapshotReplayEquivalenceTest.restoringAtAnyPointReproducesTheUninterruptedRun (all 37 scenarios incl. order_*, synth_*, core_config_*) | partial: every cut, no stride-8 large-file mode, no production-shaped lifecycle file |
| CoreSnapshotReplayEquivalenceTest.serializingTwiceYieldsTheSameBytes | determinism/SnapshotReplayEquivalenceTest.serializingTwiceYieldsTheSameBytes | |
| CoreReplicaFrameEquivalenceTest | not covered | no two-replica frame-level (demuxer + engine) equivalence test |
| CoreRunTwiceAndWallClockTest | determinism/DeterminismCorpusTest.runTwiceIsIdentical; determinism/DeterminismCorpusTest.wallClockInvariant | over the existing corpus |
| CoreGoldenTest | determinism/DeterminismCorpusTest.matchesGolden | |
| expiryOrderIsAFunctionOfStateNotHistory | time/DeadlineIndexTest.theListingIsAFunctionOfTheEntriesNotTheInsertionOrder | partial: no 40-GTD scrambled splice scenario by that name |
| stopTriggerOrderAtOnePriceIsAFunctionOfState | order/TriggerTest.allOrdersFiredByOneTickAreDispatchedInAscendingOrderId; order/SyntheticRestoreBooksTest.crossingTickOnTheRestoredEngineFiresOnlyTheNormalStop | partial: no 8-user splice scenario by that name |
| rateLimitDecisionSurvivesRestore | order/OrderSnapshotSectionsTest.ordersRiskAndRequestsSurviveRestoreWithIndexesRebuilt | partial |
| requestReplaySurvivesRestore | order/OrderSnapshotSectionsTest.ordersRiskAndRequestsSurviveRestoreWithIndexesRebuilt; order/RequestIdTest.redC1b_sameRequestSamePayloadReplaysTheDecision | partial: replay across a splice is not a dedicated test |
| rejectedCreateLeavesBytesUnchanged | order/OrderAdmissionTest.theHoldIsTheBalanceCheck_insufficientFundsLeavesOnlyTheRateCounter; persistence/IngressFenceTest.rejectedDepositWithdrawAndHoldCreateNoAccount | partial: a rate-consuming reject changes the risk block by design (S4), so "bytes unchanged" holds only for structural rejects (BehaviorSwitchSupport.refuseStructurally) |
| oneSidedTickDoesNotClearTheOtherSide | not covered | see synthetic R5: the core tick is a full top of book; an empty side is a quote of 0 by contract |

## Acceptance rows (is-paketleri.md RC1)

| id | covered by | note |
|---|---|---|
| RC1-A01 | determinism/DeterminismCorpusTest (3 checks x 37 scenarios); determinism/SnapshotReplayEquivalenceTest; splice runner methods in order/OrderSnapshotSectionsTest, order/OrderQueryAndSnapshotTest, order/SyntheticSnapshotTest, order/BehaviorSwitchTest, determinism/GateAndSessionSnapshotTest; order/OrderConservationFuzzTest; order/SyntheticFuzzTest; order/CoreSnapshotSizingTest (100k re-serialize byte-identical) | partial: the production-shaped lifecycle corpus (B4) is missing |
| RC1-A02 | order/LifecycleInvariants via order/OrderConservationFuzzTest and order/SyntheticFuzzTest; order/TerminalAndInvariantsTest.afterAnyTerminalLockedEqualsTheSumOfHoldRemainingForBothUsers; determinism/ConservationInvariantTest; order/CoreSnapshotSizingTest (I2 on 100k restored) | |
| RC1-A03 | persistence/IngressFenceTest; persistence/AssetsApplyFailureFenceTest; persistence/SnapshotEnvelopeAdoptionTest | |
| RC1-A04 | persistence/SessionAuthorityDemuxerTest; cluster/AuthenticatedClusterTest; auth/CoreAuthenticatorTest | |
| RC1-A05 | order/CoreSnapshotSizingTest (bytes, serialize/deserialize ms); cluster/CoreChunkedRestore100kClusterTest (restore-to-ready <= 30 s at 100k, chunk count, CRC refusal) | the cluster class runs only with `-Dcore.restore.100k=true` |
| RC1-A06 | order/TriggerTest.R1_buyStopLossFiresOnAskAtStopWithMarketChildBudgetEqualToHold (child budget = hold.remaining); order/FillAndTerminalTest.rC5c_overfillFillsToRemainingAndEmitsTheExcessAsAnOrderFault (integer budget + OVERFILL fault, quantity cap is RC2); order/OutcomeAndDispatchTest.rC9a_gtdTenSweepsMintOneCancelAndTheCancelOutcomeYieldsExpired (one GTD cancel); order/RequestIdTest.redC1b_sameRequestSamePayloadReplaysTheDecision (requestId replay) | |
