# Actor Pay-per-event charging example

This example demonstrates how to use the Actor Pay-per-event charging feature. It uses `ChargingManager` class and `pushDataPPEAware` function to ensure consistent charging for the actor.

This is a preview feature. Apify team is working on implementing similar functionality directly into the JS & Python SDKs.

## Source code
Available here: https://github.com/metalwarrior665/actor-charge-manager-poc

## Features
- Only charge for events defined for that Actor run
- Only charge up to user-defined `Max Total Charge Usd`
- Correctly sync charging state after restarts
- Only push dataset items that we previously charged for
- Is backward compatible with Pay-per-result billing (for pricing transition)
- Pushes metadata about each event to an unnamed metadata dataset

## Non-features
This example doesn't show how to implement a solution for the whole Crawlee crawler. In that case, you should pass `chargingManager` around in context and use `await crawler.teardown()` once the charging manager returns `eventChargeLimitReached` true (or if you cannot squeeze more events into `chargingManager.eventChargeCountTillLimit(eventId)`)

## Example run
Example run with priced events: https://console.apify.com/view/runs/WxEqtS7CQee4biw4S
