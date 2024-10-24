import { Actor, log, type ActorRun, type Dataset } from 'apify';
import type { Dataset as DatasetInfo } from 'apify-client';
// eslint-disable-next-line import/no-extraneous-dependencies
import { got, type HTTPError } from 'got-scraping';

// TODO: Parse possible error issues
const chargeRequest = async <ChargeEventId extends string>(event: ChargeEventId, count: number): Promise<void> => {
    const url = `${Actor.config.get('apiBaseUrl')}v2/actor-runs/${Actor.config.get('actorRunId')}/charge?token=${Actor.config.get('token')}`;
    const now = Date.now();
    await got.post(url, {
        retry: {
            limit: 5,
        },
        json: {
            eventName: event,
            count,
        },
        headers: {
            'idempotency-key': `${process.env.ACTOR_RUN_ID}-${event}-${now}`,
        },
    }).catch((err: HTTPError) => {
        log.error('The charging request failed with the following message', { message: err.message });
    });
};

/**
 * We get this from API under `pricingInfo` key when querying run info, if run is PPE
 */
interface ApifyApiPricingInfo<ChargeEventId extends string> {
    pricingModel: 'PAY_PER_EVENT',
    pricingPerEvent: {
        actorChargeEvents: Record<ChargeEventId, {
            eventTitle: string,
            eventDescription: string,
            eventPriceUsd: number,
        }>
    },
}

/**
 * Alongside `pricingInfo` (`PricingInfoPPE`), run info from API will contain this under `chargedEventCounts` key, if run is PPE
 */
type ApifyApiChargedEventCounts<ChargeEventId extends string> = Record<ChargeEventId, number>;

export interface ActorRunCorrectType<ChargeEventId extends string> extends ActorRun {
    pricingInfo?: ApifyApiPricingInfo<ChargeEventId>,
    chargedEventCounts?: ApifyApiChargedEventCounts<ChargeEventId>,
    options: ActorRun['options'] & {
        maxTotalChargeUsd?: number,
    }
}

type ChargeState<ChargeEventId extends string> = Record<ChargeEventId, { chargeCount: number, eventPriceUsd: number, eventTitle: string }>;

interface ChargeResult {
    chargedCount: number,
    eventChargeLimitReached: boolean,
    outcome: 'event_not_registered' | 'charge_limit_reached' | 'charge_successful',
}

/**
 * Handles everything related to PPE (Price Per Event)
 */
export class ChargingManager<ChargeEventId extends string> {
    /**
     * Can be infinity if not specified by the user
     */
    private readonly maxTotalChargeUsd: number = Infinity;
    /**
     * If PPE is on, contains info on how much each event costs and how many times it was charged for;
     * Will only contain events relevant to the current miniactor
     * This is loaded from run endpoint at start and then incremented in memory
     */
    private chargeState: ChargeState<ChargeEventId>;
    private readonly metadataDataset: Dataset;
    private constructor(initialChargeState: ChargeState<ChargeEventId>, metadataDataset: Dataset, maxTotalChargeUsd?: number) {
        this.chargeState = initialChargeState;
        this.metadataDataset = metadataDataset;
        if (maxTotalChargeUsd) {
            this.maxTotalChargeUsd = maxTotalChargeUsd;
        }
    }

    /**
     * Queries the API to figure out the number of results pushed so far and PPE info
     * (especially useful in case of a migration or an abortion); for the global number of results,
     * also sets up persisting at the KVStore
     */
    public static async initialize<ChargeEventId extends string>(localMock?: ActorRunCorrectType<ChargeEventId>): Promise<ChargingManager<ChargeEventId>> {
        const runInfo = Actor.isAtHome()
            ? await Actor.apifyClient.run(Actor.getEnv().actorRunId!).get() as ActorRunCorrectType<ChargeEventId>
            : localMock!;

        const chargeState = {} as ChargeState<ChargeEventId>;

        if (runInfo.chargedEventCounts && runInfo.pricingInfo?.pricingPerEvent?.actorChargeEvents) {
            for (const eventId of Object.keys(runInfo.pricingInfo.pricingPerEvent.actorChargeEvents)) {
                chargeState[eventId as ChargeEventId] = {
                    chargeCount: runInfo.chargedEventCounts[eventId as ChargeEventId] ?? 0,
                    eventPriceUsd: runInfo.pricingInfo.pricingPerEvent.actorChargeEvents[eventId as ChargeEventId].eventPriceUsd,
                    eventTitle: runInfo.pricingInfo.pricingPerEvent.actorChargeEvents[eventId as ChargeEventId].eventTitle,
                };
            }
        }

        // We use unnamed dataset so it is deleted with data retention. Because of that, we have to persist its ID
        let metadataDatasetInfo = await Actor.getValue('METADATA_DATASET_INFO') as DatasetInfo | null;

        if (!metadataDatasetInfo) {
            metadataDatasetInfo = await Actor.apifyClient.datasets().getOrCreate();
            await Actor.setValue('METADATA_DATASET_INFO', metadataDatasetInfo);
        }
        const metadataDataset = await Actor.openDataset(metadataDatasetInfo.id);

        return new ChargingManager<ChargeEventId>(chargeState, metadataDataset, runInfo.options.maxTotalChargeUsd);
    }

    /**
     * How much more money PPE events can charge before reaching the max cost per run
     */
    public remainingChargeBudgetUsd(): number {
        // Infinity stays at infinity
        let remainingBudgetUsd = this.maxTotalChargeUsd;
        for (const eventId of Object.keys(this.chargeState)) {
            remainingBudgetUsd -= this.chargeState[eventId as ChargeEventId].chargeCount * this.chargeState[eventId as ChargeEventId].eventPriceUsd;
        }
        // Keeping float precision issues at bay
        return Number(remainingBudgetUsd.toFixed(6));
    }

    /**
     * How many events of a given type can still be charged for before reaching the limit;
     * If the event is not registered, returns Infinity (free of charge)
     */
    public eventChargeCountTillLimit(event: ChargeEventId): number {
        if (!this.chargeState[event]) {
            return Infinity;
        }
        // First round as Math.floor(4.9999999999999999) will incorrectly return 5
        return Math.floor(Number((this.remainingChargeBudgetUsd() / (this.chargeState[event].eventPriceUsd)).toFixed(4)));
    }

    /**
     * TODO: Make this into an event planner
     */

    /*
    public limitChargeEvents(events: ACTOR_CHARGE_EVENT[], countScheduled: boolean): ACTOR_CHARGE_EVENT[] {
        let wouldBeRemainingPrice = countScheduled ? this.remainingScheduledCostUsd : this.remainingGlobalCostUsd;
        const limitedEvents: ACTOR_CHARGE_EVENT[] = [];
        for (const event of events) {
            if (wouldBeRemainingPrice < (this.ppeChargeInfo[event]?.eventPriceUsd ?? 0)) {
                break;
            }
            wouldBeRemainingPrice -= this.ppeChargeInfo[event]?.eventPriceUsd ?? 0;
            wouldBeRemainingPrice = Number(wouldBeRemainingPrice.toFixed(4));
            limitedEvents.push(event);
        }
        return limitedEvents;
    }
        */

    /**
     * Will charge for the specified event within PPE model (no-op if not PPE or no such event is present in this miniactor).
     * Unregistered events are 'free of charge' (eventChargeLimitReached: false)
     * metadata length represent count of events to charge for (add empty objects at minimum)
     */
    public async charge(event: ChargeEventId, metadata: Record<string, unknown>[]): Promise<ChargeResult> {
        if (!this.chargeState[event]) {
            return { chargedCount: 0, outcome: 'event_not_registered', eventChargeLimitReached: false };
        }

        const remainingEventChargeCount = this.eventChargeCountTillLimit(event);
        if (remainingEventChargeCount <= 0) {
            return { chargedCount: 0, outcome: 'charge_limit_reached', eventChargeLimitReached: true };
        }

        const chargeableCount = Math.min(metadata.length, remainingEventChargeCount);
        // Locally, we just skip this but do everything else as test
        if (Actor.isAtHome()) {
            await chargeRequest<ChargeEventId>(event, chargeableCount);
        }

        this.chargeState[event].chargeCount += chargeableCount;

        const eventMetadataItems = [];
        for (let i = 0; i < chargeableCount; i++) {
            eventMetadataItems.push({
                eventId: event,
                eventTitle: this.chargeState[event].eventTitle,
                eventPriceUsd: this.chargeState[event].eventPriceUsd,
                timestamp: new Date().toISOString(),
                metadata: metadata[i],
            });
        }
        await this.metadataDataset.pushData(eventMetadataItems);

        const remainingEventChargeCountAfterCharge = this.eventChargeCountTillLimit(event);

        log.debug(`[CHARGING_MANAGER] Charged for ${chargeableCount} ${event} events, remaining events: ${remainingEventChargeCountAfterCharge} `
            + `remaining cost: ${this.remainingChargeBudgetUsd()}`);

        return { chargedCount: chargeableCount, outcome: 'charge_successful', eventChargeLimitReached: remainingEventChargeCountAfterCharge <= 0 };
    }
}

/**
 * TODO: Make this into an event planner
 */

/*
export async function limitPosts({
    posts,
    label,
    downloadOptions,
    chargingManager,
}: {
    posts: XHRResponseParserOutput[];
    label: LABEL;
    downloadOptions: Omit<DownloadParams, 'kvStoreId'>;
    chargingManager?: ChargingManager;
}): Promise<{
    posts: {
        post: XHRResponseParserOutput;
        downloadOptions: Omit<DownloadParams, 'kvStoreId'>;
    }[],
    wasLimited: boolean;
}> {
    chargingManager ??= await ChargingManager.initialize();
    // PPR
    if (chargingManager.remainingGlobalResults !== Infinity) {
        const limited = posts.slice(0, chargingManager.remainingGlobalResults).map((post) => ({ post, downloadOptions }));
        return {
            posts: limited,
            wasLimited: limited.length < posts.length,
        };
    }
    const limitedPosts: {
        post: XHRResponseParserOutput;
        downloadOptions: Omit<DownloadParams, 'kvStoreId'>;
    }[] = [];
    const projectedEventsSoFar = [];
    for (const post of posts) {
        const projectedEventsForPost = [
            LABEL_TO_CHARGE_EVENT[label] ?? ACTOR_CHARGE_EVENT.POST_DATASET_ITEM,
            ...getChargeEventsForDownloadParams(post.video, downloadOptions),
        ];
        const limitedEventCount = chargingManager.limitChargeEvents([
            ...projectedEventsSoFar,
            ...projectedEventsForPost,
        ], true).length - [...projectedEventsSoFar, ...projectedEventsForPost].length;
        const limitedEvents = limitedEventCount === 0 ? projectedEventsForPost : projectedEventsForPost.slice(0, limitedEventCount);
        if (limitedEvents.length === projectedEventsForPost.length) {
            limitedPosts.push({ post, downloadOptions });
        } else if (limitedEvents.length > 0) {
            const limitedDownloadOptions: Omit<DownloadParams, 'kvStoreId'> = {
                ...downloadOptions,
                shouldDownloadCovers: limitedEvents.includes(ACTOR_CHARGE_EVENT.COVER_DOWNLOAD),
                shouldDownloadVideos: limitedEvents.includes(ACTOR_CHARGE_EVENT.VIDEO_DOWNLOAD),
                shouldDownloadSubtitles: limitedEvents.filter((event) => event === ACTOR_CHARGE_EVENT.SUBTITLE_DOWNLOAD).length
                    === post.video.videoMeta.subtitleLinks?.length,
                shouldDownloadSlideshowImages: limitedEvents.filter((event) => event === ACTOR_CHARGE_EVENT.SLIDESHOW_IMAGE_DOWNLOAD).length
                    === post.video.slideshowImageLinks?.length,
            };
            limitedPosts.push({
                post: {
                    ...post,
                    video: updatePostMediaLinks(post.video, limitedDownloadOptions),
                },
                downloadOptions: limitedDownloadOptions,
            });
            break;
        }
        projectedEventsSoFar.push(...projectedEventsForPost);
    }
    return {
        posts: limitedPosts,
        wasLimited: limitedPosts.length < posts.length,
    };
}
*/
