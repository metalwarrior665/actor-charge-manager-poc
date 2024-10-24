// Apify SDK - toolkit for building Apify Actors (Read more at https://docs.apify.com/sdk/js/)
import { Actor } from 'apify';
import { ChargingManager, type ActorRunCorrectType } from './charging-manager.js';

interface Input {
    chargeCount: number;
    mockRunInfoLocally: ActorRunCorrectType<EventId>;
}

type EventId = 'actor-start' | 'product-result'

// The init() call configures the Actor for its environment. It's recommended to start every Actor with an init()
await Actor.init();

const { chargeCount, mockRunInfoLocally } = (await Actor.getInput<Input>())!;

const chargingManager = await ChargingManager.initialize<EventId>(mockRunInfoLocally);

const chargeResult1 = await chargingManager.charge('actor-start', [{}]);
console.log('chargeResult1');
console.dir(chargeResult1);

const metadata = [];

for (let i = 0; i < chargeCount; i++) {
    metadata.push({ index: i });
}
const chargeResult2 = await chargingManager.charge('product-result', metadata);
console.log('chargeResult2');
console.dir(chargeResult2);

// Gracefully exit the Actor process. It's recommended to quit all Actors with an exit()
await Actor.exit();
