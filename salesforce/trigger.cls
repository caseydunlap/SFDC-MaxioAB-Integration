trigger OpportunityClosedWonTrigger on Opportunity (after update) {
    for (Opportunity opp : Trigger.new) {
        if (opp.StageName == 'Closed Won' && opp.Type == 'New Logo' && Trigger.oldMap.get(opp.Id).StageName != 'Closed Won') {
            OpportunityApiCaller.sendOpportunityDataToApi(opp.Id);
        }
    }
}
