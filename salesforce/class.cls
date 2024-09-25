public class OpportunityApiCaller {
    @future(callout=true)
    public static void sendOpportunityDataToApi(String oppId) {
        
        Opportunity opp = [SELECT Id FROM Opportunity WHERE Id = :oppId LIMIT 1];
        if (opp == null) {
            return;
        }
        
        Map<String, Object> payload = new Map<String, Object>{
            'Opportunity_Id' => opp.Id
        };
        String jsonPayload = JSON.serialize(payload);
        
        HttpRequest req = new HttpRequest();
        req.setEndpoint('apiendpoint');
        req.setMethod('POST');
        req.setHeader('Content-Type', 'application/json');
        req.setBody(jsonPayload);
        
        try {
            Http http = new Http();
            HttpResponse res = http.send(req);
        } catch (Exception e) {
            System.debug('Error during API call: ' + e.getMessage());
        }
    }
}
