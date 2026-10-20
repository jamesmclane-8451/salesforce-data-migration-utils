/**
 * SL_SeasonalSurchargeResolverTest
 *
 * UCM-52679 - unit tests for SL_SeasonalSurchargeResolver.
 *
 * Scope of these tests: the resolver's own input validation, error
 * handling, bulkification guard, and System.Callable dispatch contract.
 * These tests are deterministic and self-contained (@isTest data only,
 * no reliance on org data or run order) per .claude/rules/testing.md.
 *
 * The resolver delegates actual seasonal-surcharge math to the
 * existing, independently-verified CalculationProcedureVersion v10 /
 * SL_8451_Salesforce_Pricing_Endpoint Flow (confirmed correct via live
 * end-to-end REST invocation against real MCDEV8451 data across 5
 * override/standard/fallback scenarios - see UCM-52679 implementation
 * notes). Reconstructing that pricing-procedure configuration itself
 * as isolated test-context data is out of scope for this class's unit
 * tests; instead these tests assert that SL_SeasonalSurchargeResolver
 * correctly shapes inputs, calls the Flow once per line, captures
 * either a successful or a failed interview deterministically (never
 * throwing out of resolveLines for a single bad line), and enforces
 * its own documented limits - which is the behavior this class is
 * actually responsible for.
 */
@isTest
private class SL_SeasonalSurchargeResolverTest {

    @TestSetup
    static void setup() {
        Account acct = new Account(Name = 'UCM-52679 Test Account');
        insert acct;

        Product2 prod = new Product2(
            Name = 'UCM-52679 Test Product',
            IsActive = true,
            Base_Rate_Requirement__c = 'Not Required'
        );
        insert prod;
    }

    @isTest
    static void testResolveLines_emptyInput_returnsEmptyList() {
        Test.startTest();
        List<SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> results =
            SL_SeasonalSurchargeResolver.resolveLines(new List<Object>());
        Test.stopTest();

        System.assertEquals(0, results.size(), 'Empty input should produce an empty result list.');
    }

    @isTest
    static void testResolveLines_nullInput_returnsEmptyList() {
        Test.startTest();
        List<SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> results =
            SL_SeasonalSurchargeResolver.resolveLines(null);
        Test.stopTest();

        System.assertEquals(0, results.size(), 'Null input should produce an empty result list, not an exception.');
    }

    @isTest
    static void testResolveLines_missingAccountId_capturedAsLineFailureNotException() {
        Account acct = [SELECT Id FROM Account LIMIT 1];
        Product2 prod = [SELECT Id FROM Product2 LIMIT 1];

        Map<String, Object> badLine = new Map<String, Object>{
            'lineKey' => 'line-1',
            'productId' => prod.Id,
            'quantity' => 10
            // accountId intentionally omitted - this is the negative-path assertion.
        };

        Test.startTest();
        List<SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> results =
            SL_SeasonalSurchargeResolver.resolveLines(new List<Object>{ badLine });
        Test.stopTest();

        System.assertEquals(1, results.size(), 'One input line should yield exactly one result.');
        SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult result = results[0];
        System.assertEquals('line-1', result.lineKey, 'lineKey should be echoed back for correlation.');
        System.assertEquals(false, result.success, 'Missing accountId should fail that line, not the whole batch.');
        System.assert(result.errorMessage != null && result.errorMessage.contains('accountId'),
            'Error message should clearly identify the missing required field. Actual: ' + result.errorMessage);
    }

    @isTest
    static void testResolveLines_missingProductId_capturedAsLineFailureNotException() {
        Account acct = [SELECT Id FROM Account LIMIT 1];

        Map<String, Object> badLine = new Map<String, Object>{
            'lineKey' => 'line-2',
            'accountId' => acct.Id,
            'quantity' => 10
            // productId intentionally omitted.
        };

        Test.startTest();
        List<SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> results =
            SL_SeasonalSurchargeResolver.resolveLines(new List<Object>{ badLine });
        Test.stopTest();

        System.assertEquals(1, results.size());
        System.assertEquals(false, results[0].success, 'Missing productId should fail that line.');
        System.assert(results[0].errorMessage.contains('productId'),
            'Error message should identify productId as the missing field. Actual: ' + results[0].errorMessage);
    }

    @isTest
    static void testResolveLines_mixedValidAndInvalidLines_eachResolvedIndependently() {
        Account acct = [SELECT Id FROM Account LIMIT 1];
        Product2 prod = [SELECT Id FROM Product2 LIMIT 1];

        Map<String, Object> goodLine = new Map<String, Object>{
            'lineKey' => 'good-line',
            'accountId' => acct.Id,
            'productId' => prod.Id,
            'quantity' => 5,
            'budget' => 1000,
            'isBudgetBasedQuantity' => false,
            'isAgencyBilled' => false,
            'discountAmount' => 0,
            'discountPercentage' => 0,
            'adServerFeesAndSurcharges' => 0
        };
        Map<String, Object> badLine = new Map<String, Object>{
            'lineKey' => 'bad-line'
            // both accountId and productId omitted.
        };

        Test.startTest();
        List<SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> results =
            SL_SeasonalSurchargeResolver.resolveLines(new List<Object>{ goodLine, badLine });
        Test.stopTest();

        System.assertEquals(2, results.size(), 'Both lines should produce a result, regardless of individual outcome.');

        Map<String, SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> byKey =
            new Map<String, SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult>();
        for (SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult r : results) {
            byKey.put(r.lineKey, r);
        }

        System.assert(byKey.containsKey('good-line'), 'Result set should preserve the good-line key.');
        System.assert(byKey.containsKey('bad-line'), 'Result set should preserve the bad-line key.');
        // The "good" line is structurally valid (has accountId/productId) so it will
        // reach Flow.Interview; whether the underlying pricing Flow finds a match in
        // this isolated test context is not asserted here - that end-to-end behavior
        // is independently verified live against real MCDEV8451 data. What IS asserted
        // is that this line does not fail for the structural reason the bad line does.
        System.assert(
            byKey.get('good-line').errorMessage == null || !byKey.get('good-line').errorMessage.contains('is required'),
            'The structurally valid line should not fail for a missing-required-field reason.'
        );
        System.assertEquals(false, byKey.get('bad-line').success, 'The structurally invalid line should fail.');
    }

    @isTest
    static void testResolveLines_bulkAtDocumentedLimit_doesNotThrow() {
        Account acct = [SELECT Id FROM Account LIMIT 1];
        Product2 prod = [SELECT Id FROM Product2 LIMIT 1];

        List<Object> lines = new List<Object>();
        for (Integer i = 0; i < SL_SeasonalSurchargeResolver.MAX_LINES_PER_CALL; i++) {
            lines.add(new Map<String, Object>{
                'lineKey' => 'line-' + i,
                'accountId' => acct.Id,
                'productId' => prod.Id,
                'quantity' => 1
            });
        }

        Test.startTest();
        List<SL_SeasonalSurchargeResolver.SeasonalSurchargeLineResult> results =
            SL_SeasonalSurchargeResolver.resolveLines(lines);
        Test.stopTest();

        System.assertEquals(SL_SeasonalSurchargeResolver.MAX_LINES_PER_CALL, results.size(),
            'Exactly the documented maximum number of lines should be accepted and resolved, confirming bulk behavior up to the stated limit.');
    }

    @isTest
    static void testResolveLines_overDocumentedLimit_throwsClearException() {
        Account acct = [SELECT Id FROM Account LIMIT 1];
        Product2 prod = [SELECT Id FROM Product2 LIMIT 1];

        List<Object> lines = new List<Object>();
        for (Integer i = 0; i < SL_SeasonalSurchargeResolver.MAX_LINES_PER_CALL + 1; i++) {
            lines.add(new Map<String, Object>{
                'lineKey' => 'line-' + i,
                'accountId' => acct.Id,
                'productId' => prod.Id,
                'quantity' => 1
            });
        }

        Boolean threw = false;
        String message;

        Test.startTest();
        try {
            SL_SeasonalSurchargeResolver.resolveLines(lines);
        } catch (SL_SeasonalSurchargeResolver.SeasonalSurchargeResolverException ex) {
            threw = true;
            message = ex.getMessage();
        }
        Test.stopTest();

        System.assertEquals(true, threw, 'Exceeding the documented per-call line limit should throw a clear, typed exception rather than silently truncating or failing with a governor-limit error.');
        System.assert(message.contains(String.valueOf(SL_SeasonalSurchargeResolver.MAX_LINES_PER_CALL)),
            'Exception message should state the documented limit. Actual: ' + message);
    }

    @isTest
    static void testCall_dispatchesToResolveSeasonalSurcharge() {
        Account acct = [SELECT Id FROM Account LIMIT 1];
        Product2 prod = [SELECT Id FROM Product2 LIMIT 1];

        Map<String, Object> line = new Map<String, Object>{
            'lineKey' => 'omni-line-1',
            'accountId' => acct.Id,
            'productId' => prod.Id,
            'quantity' => 1
        };
        Map<String, Object> input = new Map<String, Object>{ 'lines' => new List<Object>{ line } };
        Map<String, Object> output = new Map<String, Object>();
        Map<String, Object> options = new Map<String, Object>();
        Map<String, Object> args = new Map<String, Object>{
            'input' => input,
            'output' => output,
            'options' => options
        };

        SL_SeasonalSurchargeResolver resolver = new SL_SeasonalSurchargeResolver();

        Test.startTest();
        Object callResult = resolver.call('resolveSeasonalSurcharge', args);
        Test.stopTest();

        System.assertEquals(true, callResult, 'call() should return true on successful dispatch, matching the OmniStudio Remote Action contract.');
        System.assert(output.containsKey('results'), 'output map should be populated with a "results" key for the OmniProcess Remote Action step to consume.');
        List<Object> resultList = (List<Object>) output.get('results');
        System.assertEquals(1, resultList.size(), 'One input line should produce one serialized result entry.');
    }

    @isTest
    static void testCall_singleMapShapedLine_normalizedToList() {
        // OmniStudio's Loop Block output collapses to a bare Map (not a
        // single-element List) when a repeating block produces exactly one
        // iteration - e.g. a product with only one Billable Account and no
        // split. This reproduces that exact "lines" shape from the Quote-UI
        // path (UCM-52679 AC6) and asserts it no longer throws
        // System.TypeException: Invalid conversion from runtime type
        // Map<String,ANY> to List<ANY>.
        Account acct = [SELECT Id FROM Account LIMIT 1];
        Product2 prod = [SELECT Id FROM Product2 LIMIT 1];

        Map<String, Object> singleLine = new Map<String, Object>{
            'lineKey' => 'single-line',
            'accountId' => acct.Id,
            'productId' => prod.Id,
            'quantity' => 1
        };
        Map<String, Object> input = new Map<String, Object>{ 'lines' => singleLine };
        Map<String, Object> output = new Map<String, Object>();
        Map<String, Object> options = new Map<String, Object>();
        Map<String, Object> args = new Map<String, Object>{
            'input' => input,
            'output' => output,
            'options' => options
        };

        SL_SeasonalSurchargeResolver resolver = new SL_SeasonalSurchargeResolver();

        Test.startTest();
        Object callResult = resolver.call('resolveSeasonalSurcharge', args);
        Test.stopTest();

        System.assertEquals(true, callResult, 'call() should succeed even when "lines" is a bare Map, not a List.');
        System.assert(output.containsKey('results'), 'output map should still be populated with "results".');
        List<Object> resultList = (List<Object>) output.get('results');
        System.assertEquals(1, resultList.size(), 'The single Map-shaped line should normalize to one result entry.');
    }

    @isTest
    static void testNormalizeLines_listMapNullAndUnsupportedShape() {
        System.assertEquals(0, SL_SeasonalSurchargeResolver.normalizeLines(null).size(),
            'Null input should normalize to an empty list.');

        List<Object> alreadyList = new List<Object>{ new Map<String, Object>{ 'a' => 1 } };
        System.assertEquals(alreadyList, SL_SeasonalSurchargeResolver.normalizeLines(alreadyList),
            'A List input should be returned as-is.');

        Map<String, Object> singleMap = new Map<String, Object>{ 'a' => 1 };
        List<Object> normalized = SL_SeasonalSurchargeResolver.normalizeLines(singleMap);
        System.assertEquals(1, normalized.size(), 'A single Map input should normalize to a one-element list.');
        System.assertEquals(singleMap, normalized[0], 'The wrapped element should be the original Map.');

        Boolean threw = false;
        try {
            SL_SeasonalSurchargeResolver.normalizeLines('not a map or list');
        } catch (SL_SeasonalSurchargeResolver.SeasonalSurchargeResolverException ex) {
            threw = true;
        }
        System.assertEquals(true, threw, 'An unsupported "lines" shape should throw a clear typed exception.');
    }

    @isTest
    static void testCall_unsupportedAction_throws() {
        Map<String, Object> args = new Map<String, Object>{
            'input' => new Map<String, Object>(),
            'output' => new Map<String, Object>(),
            'options' => new Map<String, Object>()
        };

        SL_SeasonalSurchargeResolver resolver = new SL_SeasonalSurchargeResolver();

        Boolean threw = false;
        Test.startTest();
        try {
            resolver.call('someUnknownAction', args);
        } catch (SL_SeasonalSurchargeResolver.SeasonalSurchargeResolverException ex) {
            threw = true;
        }
        Test.stopTest();

        System.assertEquals(true, threw, 'An unrecognized action name must raise a clear typed exception, not fail silently or NPE.');
    }

    @isTest
    static void testParseFlightDate_stringAndNullAndDateTimeShapes() {
        // OmniStudio IP/DataRaptor inputs deserialize dates as plain Strings
        // (e.g. "2026-07-01"), not Apex DateTime instances - a direct
        // (DateTime) cast on that String throws System.TypeException at
        // runtime, which was silently breaking every live OmniScript
        // invocation of this resolver (UCM-52679 AC6).
        System.assertEquals(null, SL_SeasonalSurchargeResolver.parseFlightDate(null),
            'Null input should return null.');

        DateTime fromShortString = SL_SeasonalSurchargeResolver.parseFlightDate('2026-07-01');
        System.assertEquals(Date.newInstance(2026, 7, 1), fromShortString.dateGmt(),
            'A plain yyyy-MM-dd String should parse to that calendar date.');

        DateTime original = DateTime.newInstance(2026, 7, 1, 0, 0, 0);
        System.assertEquals(original, SL_SeasonalSurchargeResolver.parseFlightDate(original),
            'An actual DateTime input should be returned unchanged.');

        Date originalDate = Date.newInstance(2026, 7, 1);
        System.assertEquals(originalDate, SL_SeasonalSurchargeResolver.parseFlightDate(originalDate).dateGmt(),
            'An actual Date input should convert cleanly to DateTime.');
    }
}