import path from "path";
import { AdminUiExtension } from "@vendure/ui-devkit/compiler";
import {
  EventBus,
  LanguageCode,
  PluginCommonModule,
  ProductEvent,
  ProductVariantEvent,
  VendurePlugin,
} from "@vendure/core";
import { ProductFeedBuilderService } from "./service/product-feed-builder.service";
import "./types";
import { ProductController } from "./api/product-feed.controller";
import { ProductFeedPluginOptions } from "./types";
import { PLUGIN_INIT_OPTIONS, productFeedPerm } from "./constants";
import { adminApiExtensions } from "./api/api-extentions";
import { ProductAdminResolver } from "./api/product-feed.admin.resolver";
import { ProductFeedUpdatedEvent } from "./events/product-feed-updated";
import { ProductFeedUploadService } from "./service/product-feed-upload.service";

@VendurePlugin({
  imports: [PluginCommonModule],
  adminApiExtensions: {
    schema: adminApiExtensions,
    resolvers: [ProductAdminResolver],
  },
  providers: [
    ProductFeedBuilderService,
    ProductFeedUploadService,
    {
      provide: PLUGIN_INIT_OPTIONS,
      useFactory: () => ProductFeedPlugin.options,
    },
  ],
  controllers: [ProductController],
  compatibility: ">2.0.0",
  configuration: (config) => {
    config.authOptions.customPermissions.push(productFeedPerm);

    config.customFields.Channel.push({
      type: "string",
      name: "productFeedShopUrl",
      label: [
        {
          languageCode: LanguageCode.en,
          value: "Shop URL",
        },
      ],
      public: false,
      ui: { tab: "Product feed" },
    });

    config.customFields.Channel.push({
      type: "string",
      name: "productFeedOutput",
      label: [
        {
          languageCode: LanguageCode.en,
          value: "Product feed",
        },
      ],
      options: [
        {
          value: "disabled",
          label: [{ languageCode: LanguageCode.en, value: "Disabled" }],
        },
        {
          value: "url",
          label: [{ languageCode: LanguageCode.en, value: "URL" }],
        },
        {
          value: "sftp",
          label: [{ languageCode: LanguageCode.en, value: "SFTP" }],
        },
      ],
      defaultValue: "disabled",
      public: false,
      ui: { tab: "Product feed" },
    });

    config.customFields.Channel.push({
      type: "string",
      name: "productFeedSftpServer",
      label: [
        {
          languageCode: LanguageCode.en,
          value: "SFTP server",
        },
      ],
      public: false,
      ui: { tab: "Product feed" },
    });

    config.customFields.Channel.push({
      type: "int",
      name: "productFeedSftpPort",
      min: 1,
      max: 65535,
      label: [
        {
          languageCode: LanguageCode.en,
          value: "SFTP port",
        },
      ],
      defaultValue: 22,
      public: false,
      ui: { tab: "Product feed" },
    });

    config.customFields.Channel.push({
      type: "string",
      name: "productFeedSftpUser",
      label: [
        {
          languageCode: LanguageCode.en,
          value: "SFTP user",
        },
      ],
      public: false,
      ui: { tab: "Product feed" },
    });

    config.customFields.Channel.push({
      type: "string",
      name: "productFeedSftpPassword",
      label: [
        {
          languageCode: LanguageCode.en,
          value: "SFTP password",
        },
      ],
      public: false,
      ui: { tab: "Product feed", component: "password-form-input" },
    });

    config.customFields.Channel.push({
      type: "boolean",
      name: "productFeedRebuild",
      defaultValue: false,
      internal: true,
    });
    config.customFields.Channel.push({
      type: "string",
      name: "productFeedFile",
      internal: true,
    });

    return config;
  },
})
export class ProductFeedPlugin {
  static options: ProductFeedPluginOptions;

  static init(
    options: Partial<ProductFeedPluginOptions> &
      Pick<ProductFeedPluginOptions, "assetUrlPrefix" | "feedStrategy">
  ): typeof ProductFeedPlugin {
    this.options = {
      productUrl: (shop, variant) => `${shop}/${variant.product.slug}`,
      ...options,
    };
    return ProductFeedPlugin;
  }

  static ui: AdminUiExtension = {
    id: "product-feed-extentions",
    extensionPath: path.join(__dirname, "ui"),
    ngModules: [
      {
        type: "shared",
        ngModuleFileName: "product-feed.module.ts",
        ngModuleName: "ProductFeedModule",
      },
    ],
  };

  constructor(
    private eventBus: EventBus,
    private productFeedBuilderService: ProductFeedBuilderService,
    private productFeedUploadService: ProductFeedUploadService
  ) {}

  async onApplicationBootstrap() {
    this.eventBus.ofType(ProductEvent).subscribe(async (event) => {
      return this.productFeedBuilderService.markChannelForRebuild(
        event.ctx
      );
    });

    this.eventBus.ofType(ProductVariantEvent).subscribe((event) => {
      return this.productFeedBuilderService.markChannelForRebuild(
        event.ctx
      );
    });

    this.eventBus.ofType(ProductFeedUpdatedEvent).subscribe((event) => {
      if(event.ctx.channel.customFields.productFeedOutput === 'sftp') {
        return this.productFeedUploadService.uploadToSftp(
          event.ctx.channel,
          event.filePath,
          event.fileName
        );
      }
    });
  }
}
