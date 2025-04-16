# Vendure product feed plugin

This plugin generates an product feed of all products. This file can be exposed on an endpoint or uploaded to an SFTP server.

When a product or variant is changed, the channel will be marked as changed.
On the product overview page is a button to trigger the rebuild of the feed output.
Or configure a cron that runs every night and rebuilds all the channels that are marked as changed.

## Requirements

Install the Product feed plugin:

```yarn add @codibri/vendure-product-feed-plugin```
or
```npm install @codibri/vendure-product-feed-plugin```

## Setup

1. Add the plugin to your VendureConfig plugins array and add `ProductFeedPlugin.ui` to the AdminUi plugin

```typescript
import { ProductFeedPlugin, GoogleProductFeedStrategy } from "@codibri/vendure-product-feed-plugin";

// ...

plugins: [
  AdminUiPlugin.init({
    port: 3002,
    route: "admin",
    app: compileUiExtensions({
      outputPath: path.join(__dirname, "../admin-ui"),
      extensions: [ProductFeedPlugin.ui],
      devMode: true,
    }),
  }),
  ProductFeedPlugin.init({
    assetUrlPrefix: "https://<vendure-server>/assets",
    productUrl: (shopUrl, variant) => `${shopUrl}/product/${variant.product.slug}`,
    folder: "custom-product-feed",
    feedStrategy: {
      getFileName: (channel) => `google-${channel.code}`,
      strategy: GoogleProductFeedStrategy,
    },
  }),
];
```

## Plugin options

### Options

| key            | required | default value   | description                                   |
| -------------- | -------- | --------------- | --------------------------------------------- |
| assetUrlPrefix | yes      |                 | The url to access the Vendure server          |
| feedStrategy   | yes      |                 | Feed strategy config                          |
| productUrl     | no       |                 | Function that returns the url for a variant.  |
| folder         | no       | product-catalog | The folder in assets where the feed is stored |

### Feed strategy config

| key         | required | default value | description                        |
| ----------- | -------- | ------------- | ---------------------------------- |
| getFileName | yes      |               | Function that returns the filename |
| strategy    | yes      |               | Prouct feed strategy               |

## Config

In the Admin UI you can configure each channel individually.
Go to the channel config in the Admin UI and fill in the Shop URL, Output and the SFTP fields in case you selected SFTP in the output.

| Field           | Description                                                                                                                                                                                                                                |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Shop URL        | The url of the shop front end.                                                                                                                                                                                                             |
| Product catalog | <ul><li>**Disabled**: No product feed will be generated for this channel</li><li></li>**URL**: The XML file will be build and availble via an url<li>**SFTP**: The XML file will be build and uploaded to configured SFTP server</li></ul> |
| SFTP server     | Only when **SFTP** is selected as output                                                                                                                                                                                                   |
| SFTP port       | Only when **SFTP** is selected as output. Must be between 1 and 65535                                                                                                                                                                      |
| SFTP user       | Only when **SFTP** is selected as output                                                                                                                                                                                                   |
| SFTP password   | Only when **SFTP** is selected as output                                                                                                                                                                                                   |

## Strategies

At the moment we only provide one output strategy. You can create your own by extending `ProductFeedStrategy`.

### Google product feed

Build an XML to upload your products to Google Merchant Center.

## API endpoint

When **URL** is selected in the channel config, the generated XML is available via the `/feed` endpoint on the server.

If you have multiple channel, you can provide the channel token in the header or as query parameter.
`/feed?token=<channelToken>`

## Building the feed

### Run mannually

On the products overview page in the Admin UI you will find a new button "Rebuild product catalog" to trigger the build of the product catalog feed of the current selected channel.

An extra permission `ProductCatalogFeedRebuild` is added to control who can accees the manual trigger button on the products page.
If no product feed is configured for the channel, the button will not be visible.

### Run automatically

Create a script to check all channels that needs to be rebuild. Use a job runner or cron job to run this script.

```typescript
// product-catalog-feed.ts

import { bootstrap } from "@vendure/core";
import { ProductCatalogFeedService } from "@codibri/vendure-plugin-product-catalog-feed";
import { config } from "./vendure-config";

bootstrap(config)
  .then((app) => app.get(ProductCatalogFeedService).buildAllFeeds())
  .catch((err) => {
    console.log(err);
  });
```
