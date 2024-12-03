// Run this locally by injecting my test run ID: ACTOR_RUN_ID=kbsnsIf2r8xC5HUaL apify run
// or inject your own run ID after you set PPE pricing to your Actor
// You can also check real PPE run here: https://console.apify.com/view/runs/WxEqtS7CQee4biw4S

import { Actor, log } from 'apify';
// Charging manager ensures that we don't charge more than max total charge USD for the run
// It also tells us how many events we actually charged for (if we hit the limit)
import { ChargingManager } from './charging-manager.js';
// pushDataPPEMaxAware uses charging manager to ensure we push only as much as we can afford
// It also fallbacks to PPR if PPE is not enabled yet, this is useful for the transition period
import { pushDataPPEAware } from './push-data-ppe.js';

export type EventId = 'actor-start-gb' | 'product-result'

await Actor.init();

log.setLevel(log.LEVELS.DEBUG);

console.dir(process.env)

// To test your implementation locally, you can create a dummy Actor on Apify, set PPE pricing to it, run it
// and then start the local run with `ACTOR_RUN_ID=your_id apify run`

// We want to charge this event only once, even if the Actor migrates or is resurrected
if ((await ChargingManager.chargedEventCount<EventId>('actor-start-gb')) === 0) {
    // We charge X times based on number of GBs the run was started with
    // This is to motivate users to run with low memory unless they really need more for larger runs
    const actorRunGBs = Math.ceil((Actor.getEnv().memoryMbytes!) / 1024);
    const actorStartEventsMetadata = Array.from({ length: actorRunGBs }, () => ({}));
    // `actor-start-gb` event doesn't push anything to dataset so we call the charging manager directly
    // Each metadata object counts as a separate charge event, so e.g. 4 empty objects will charge 4 times for 1 GB
    const chargeResultStart = await ChargingManager.charge<EventId>('actor-start-gb', actorStartEventsMetadata);
    console.log('Charge result for actor-start-gb');
    console.dir(chargeResultStart);
}

// We assume the $ budget is at least for start and some item events
// so we don't check here if we should stop

// Now we will push some items where each item will represent one charge event
// Since this is a common action, we will use a helper function that will also take care
// of transitioning from PPR to PPE
const DUMMY_ITEMS = Array.from({ length: 5 }, (_, i) => ({ itemIndex: i }));

const { eventChargeLimitReached } = await pushDataPPEAware(DUMMY_ITEMS, 'product-result');

// We can use the shouldStop flag to determine if we should stop our Actor or there is still budget to continue
if (eventChargeLimitReached) {
    await Actor.exit(`Stopping Actor because we reached the max total charge of ${ChargingManager.maxTotalChargeUsd}`);
}

// Imagine more code here that pushes more items
// ...

await Actor.exit();
