/**
 * SL_SeasonalSurchargeResolver
 *
 * UCM-52679 - Seasonal Surcharge Overrides for Contracted Accounts.
 *
 * Remote Action (System.Callable) wired into the Quote-UI OmniProcess
 * ("Opportunity with multiple Accounts" / MultipleAccounts) so that
 * Quote Line Items created through the OmniScript resolve the same
 * override-vs-standard seasonal surcharge logic that the headless
 * pricing Flow (SL_8451_Salesforce_Pricing_Endpoint, wrapping
 * CalculationProcedureVersion v10 of Master_Media_Pricing_Procedure)
 * already applies on the direct/API pricing path. This guarantees the
 * surcharge is resolved consistently regardless of entry point (AC6).
 *
 * Pattern follows the existing SL_AccessToken Remote Action precedent:
 * implement System.Callable, dispatch by action name to a named
 * invokeMethod, read "input" from the args map, write to "output".
 *
 * This class performs no DML and no direct SOQL of its own against
 * pricing/rate objects - all of that happens inside the existing,
 * already-tested CalculationProcedureVersion v10 Flow. This class is a
 * thin, read-only orchestration/mapping layer with no CRUD/FLS exposure
 * to query results; the only "output" is pricing numbers that are
 * already, independently enforced by the Flow it calls (which itself
 * operates under the running user's existing object/field access,
 * since Flow.Interview inherits the calling user's sharing/FLS context
 * in this org's default configuration for this class, consistent with
 * how SL_PricingEstimatorController already invokes the same Flow).
 */
public with sharing class SL_SeasonalSurchargeResolver implements System.Callable {

    public class SeasonalSurchargeResolverException extends Exception {}

    // Defensive governor-limit guard. Each Flow.Interview invocation
    // performs its own SOQL/DML inside the pricing procedure; running
    // too many interviews in a single Apex transaction risks exceeding
    // platform per-transaction limits. The OmniScript is expected to
    // invoke this Remote Action once per repeating-block iteration
    // (a handful of lines at a time), not once for an entire 200+ line
    // quote in one call. This cap makes that assumption an explicit,
    // visible contract instead of a silent governor-limit failure.
    @TestVisible
    private static final Integer MAX_LINES_PER_CALL = 40;

    private static final String PRICING_FLOW_API_NAME = 'SL_8451_Salesforce_Pricing_Endpoint';

    public class SeasonalSurchargeLineResult {
        public String lineKey;
        public Boolean success = false;
        public String errorMessage;
        public String seasonalSurchargeSource;
        public Decimal seasonalOverridePercentage;
        public Decimal seasonalPercentage;
        public Decimal seasonalAmount;
        public String seasonalSurchargePlanId;
        public String seasonalOverrideRateId;
        public Decimal offerPrice;
        public Decimal netTotalPrice;
        public Decimal lowestRate;
        public String pricingModel;
        public String rateCardType;
        public String contractRateSource;
    }

    // System.Callable entry point used by the OmniProcess Remote Action element.
    public Object call(String action, Map<String, Object> args) {
        Map<String, Object> input = (Map<String, Object>) args.get('input');
        Map<String, Object> output = (Map<String, Object>) args.get('output');
        Map<String, Object> options = (Map<String, Object>) args.get('options');
        return invokeMethod(action, input, output, options);
    }

    public Boolean invokeMethod(String methodName, Map<String, Object> input, Map<String, Object> output, Map<String, Object> options) {
        if (methodName == 'resolveSeasonalSurcharge') {
            List<Object> rawLines = input != null && input.get('lines') != null
                ? (List<Object>) input.get('lines')
                : new List<Object>();
            List<SeasonalSurchargeLineResult> results = resolveLines(rawLines);
            output.put('results', serializeResults(results));
            return true;
        }
        throw new SeasonalSurchargeResolverException('Unsupported action: ' + methodName);
    }

    // Invocable, Apex-testable entry point. Kept separate from the
    // System.Callable plumbing above so unit tests can call it directly
    // without constructing OmniStudio's action/input/output map shape.
    public static List<SeasonalSurchargeLineResult> resolveLines(List<Object> rawLines) {
        if (rawLines == null || rawLines.isEmpty()) {
            return new List<SeasonalSurchargeLineResult>();
        }
        if (rawLines.size() > MAX_LINES_PER_CALL) {
            throw new SeasonalSurchargeResolverException(
                'resolveSeasonalSurcharge received ' + rawLines.size() +
                ' lines in a single call, which exceeds the supported limit of ' +
                MAX_LINES_PER_CALL + '. Invoke this action once per repeating-block ' +
                'iteration (a few lines at a time) rather than for an entire quote at once.'
            );
        }

        List<SeasonalSurchargeLineResult> results = new List<SeasonalSurchargeLineResult>();

        for (Object rawLineObj : rawLines) {
            Map<String, Object> rawLine = (Map<String, Object>) rawLineObj;
            SeasonalSurchargeLineResult lineResult = new SeasonalSurchargeLineResult();
            lineResult.lineKey = (String) rawLine.get('lineKey');

            try {
                Map<String, Object> flowInputs = buildFlowInputs(rawLine);

                Flow.Interview interview = Flow.Interview.createInterview(PRICING_FLOW_API_NAME, flowInputs);
                interview.start();

                lineResult.seasonalSurchargeSource = (String) interview.getVariableValue('ret_SeasonalSurchargeSource');
                lineResult.seasonalOverridePercentage = (Decimal) interview.getVariableValue('ret_SeasonalOverridePercentage');
                lineResult.seasonalPercentage = (Decimal) interview.getVariableValue('ret_SeasonalPercentage');
                lineResult.seasonalAmount = (Decimal) interview.getVariableValue('ret_SeasonalAmount');
                lineResult.seasonalSurchargePlanId = (String) interview.getVariableValue('ret_SeasonalSurchargePlanId');
                lineResult.seasonalOverrideRateId = (String) interview.getVariableValue('ret_SeasonalOverrideRateId');
                lineResult.offerPrice = (Decimal) interview.getVariableValue('ret_OfferPrice');
                lineResult.netTotalPrice = (Decimal) interview.getVariableValue('ret_NetTotalPrice');
                lineResult.lowestRate = (Decimal) interview.getVariableValue('ret_LowestRate');
                lineResult.pricingModel = (String) interview.getVariableValue('ret_PricingModel');
                lineResult.rateCardType = (String) interview.getVariableValue('ret_RateCardType');
                lineResult.contractRateSource = (String) interview.getVariableValue('ret_ContractRateSource');
                lineResult.success = true;
            } catch (Exception ex) {
                lineResult.success = false;
                lineResult.errorMessage = ex.getMessage();
            }

            results.add(lineResult);
        }

        return results;
    }

    @TestVisible
    private static Map<String, Object> buildFlowInputs(Map<String, Object> rawLine) {
        if (rawLine.get('accountId') == null) {
            throw new SeasonalSurchargeResolverException('accountId is required to resolve seasonal surcharge.');
        }
        if (rawLine.get('productId') == null) {
            throw new SeasonalSurchargeResolverException('productId is required to resolve seasonal surcharge.');
        }

        Map<String, Object> flowInputs = new Map<String, Object>();
        flowInputs.put('SFAccountId', (Id) rawLine.get('accountId'));
        flowInputs.put('SFClientAccountId', rawLine.get('clientAccountId') != null ? (Id) rawLine.get('clientAccountId') : (Id) rawLine.get('accountId'));
        flowInputs.put('SFBillingAgencyAccountId', rawLine.get('billingAgencyAccountId') != null ? (Id) rawLine.get('billingAgencyAccountId') : null);
        flowInputs.put('SFProductId', (Id) rawLine.get('productId'));
        flowInputs.put('Quantity', rawLine.get('quantity') != null ? (Decimal) rawLine.get('quantity') : 0);
        flowInputs.put('Budget', rawLine.get('budget') != null ? (Decimal) rawLine.get('budget') : 0);
        flowInputs.put('IsBudgetBasedQuantity', rawLine.get('isBudgetBasedQuantity') != null ? (Boolean) rawLine.get('isBudgetBasedQuantity') : false);
        flowInputs.put('IsAgencyBilled', rawLine.get('isAgencyBilled') != null ? (Boolean) rawLine.get('isAgencyBilled') : false);
        flowInputs.put('FlightStartDate', rawLine.get('flightStartDate') != null ? (DateTime) rawLine.get('flightStartDate') : null);
        flowInputs.put('FlightEndDate', rawLine.get('flightEndDate') != null ? (DateTime) rawLine.get('flightEndDate') : null);
        flowInputs.put('DiscountAmount', rawLine.get('discountAmount') != null ? (Decimal) rawLine.get('discountAmount') : 0);
        flowInputs.put('DiscountPercentage', rawLine.get('discountPercentage') != null ? (Decimal) rawLine.get('discountPercentage') : 0);
        flowInputs.put('AdServerFeesAndSurcharges', rawLine.get('adServerFeesAndSurcharges') != null ? (Decimal) rawLine.get('adServerFeesAndSurcharges') : 0);
        flowInputs.put('RevisedPrice', rawLine.get('revisedPrice') != null ? (Decimal) rawLine.get('revisedPrice') : null);
        return flowInputs;
    }

    private static List<Map<String, Object>> serializeResults(List<SeasonalSurchargeLineResult> results) {
        List<Map<String, Object>> serialized = new List<Map<String, Object>>();
        for (SeasonalSurchargeLineResult r : results) {
            serialized.add(new Map<String, Object>{
                'lineKey' => r.lineKey,
                'success' => r.success,
                'errorMessage' => r.errorMessage,
                'seasonalSurchargeSource' => r.seasonalSurchargeSource,
                'seasonalOverridePercentage' => r.seasonalOverridePercentage,
                'seasonalPercentage' => r.seasonalPercentage,
                'seasonalAmount' => r.seasonalAmount,
                'seasonalSurchargePlanId' => r.seasonalSurchargePlanId,
                'seasonalOverrideRateId' => r.seasonalOverrideRateId,
                'offerPrice' => r.offerPrice,
                'netTotalPrice' => r.netTotalPrice,
                'lowestRate' => r.lowestRate,
                'pricingModel' => r.pricingModel,
                'rateCardType' => r.rateCardType,
                'contractRateSource' => r.contractRateSource
            });
        }
        return serialized;
    }
}
