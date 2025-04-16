import { Inject, Injectable, OnModuleInit } from "@nestjs/common";
import {
  Channel,
  ConfigService,
  EventBus,
  ID,
  JobQueue,
  JobQueueService,
  Logger,
  ProductPriceApplicator,
  ProductVariant,
  RequestContext,
  RequestContextService,
  StockLevelService,
  TransactionalConnection,
  TranslatorService,
  asyncObservable,
} from "@vendure/core";
import stream from "stream";
import { PLUGIN_INIT_OPTIONS, loggerCtx } from "../constants";
import { ProductFeedPluginOptions } from "../types";
import { ProductFeedUpdatedEvent } from "../events/product-feed-updated";

export const BATCH_SIZE = 1000;

type BuildFeedReponse = {
  total: number;
  completed: number;
};

@Injectable()
export class ProductFeedBuilderService implements OnModuleInit {
  private jobQueue: JobQueue<{
    channelId: ID;
  }>;

  constructor(
    @Inject(PLUGIN_INIT_OPTIONS)
    private options: ProductFeedPluginOptions,
    private jobQueueService: JobQueueService,
    private connection: TransactionalConnection,
    private requestContextService: RequestContextService,
    private configService: ConfigService,
    private stockLevelService: StockLevelService,
    private translator: TranslatorService,
    private productPriceApplicator: ProductPriceApplicator,
    private eventBus: EventBus
  ) {}

  async onModuleInit() {
    this.jobQueue = await this.jobQueueService.createQueue({
      name: "product-feed-build",
      process: async (job) => {
        const ob = this.buildChannelFeed(job.data.channelId);

        return new Promise((resolve, reject) => {
          let total: number | undefined;
          let completed = 0;
          ob.subscribe({
            next: (response: BuildFeedReponse) => {
              if (!total) {
                total = response.total;
              }
              completed = response.completed;
              const progress =
                total === 0 ? 100 : Math.ceil((completed / total) * 100);
              job.setProgress(progress);
            },
            complete: () => {
              resolve({
                success: true,
                totalProductsCount: total,
              });
            },
            error: (err: any) => {
              Logger.error(
                err.message || JSON.stringify(err),
                undefined,
                err.stack
              );
              reject(err);
            },
          });
        });
      },
    });
  }

  public async markChannelForRebuild(ctx: RequestContext) {
    const channel = ctx.channel;

    if (channel.customFields.productFeedOutput == "disabled") {
      return;
    }

    const rebuildFeed = channel.customFields.productFeedRebuild;

    if (!rebuildFeed) {
      Logger.info(
        `Mark channel (ID: ${channel.id} ) for rebduilding product feed`
      );
      channel.customFields.productFeedRebuild = true;
      await this.connection.getRepository(ctx, Channel).save(channel);
    }
  }

  public async buildAllFeeds() {
    Logger.verbose("Checking channels to build", loggerCtx);

    const channels = await this.connection.rawConnection
      .getRepository(Channel)
      .find({ where: { customFields: { productFeedRebuild: true } } });

    channels.forEach((channel) => {
      Logger.verbose(`Rebuild channel "${channel.code}"`);
      this.addChannelRebuildToQueue(channel.id);
    });
  }

  public addChannelRebuildToQueue(channelId: ID) {
    return this.jobQueue.add({ channelId });
  }

  private buildChannelFeed(channelId: ID) {
    return asyncObservable<BuildFeedReponse>(async (observer) => {
      const channel = await this.connection.rawConnection
        .getRepository(Channel)
        .findOneOrFail({
          where: { id: channelId },
          relations: ["defaultTaxZone"],
        });

      Logger.verbose(`Start building feed for channel ${channel.code}`);

      const ctx = await this.requestContextService.create({
        apiType: "custom",
        channelOrToken: channel,
      });

      const qb = this.connection
        .getRepository(ctx, ProductVariant)
        .createQueryBuilder("variants")
        .setFindOptions({
          relations: [
            "translations",
            "taxCategory",
            "featuredAsset",
            "product",
            "product.featuredAsset",
          ],
          loadEagerRelations: true,
        })
        .leftJoin("variants.product", "product")
        .leftJoin("product.channels", "channel")
        .where("channel.id = :channelId", { channelId });

      const count = await qb.getCount();
      Logger.verbose(
        `Building product  feed. Found ${count} variants for channel ${ctx.channel.code}`,
        loggerCtx
      );

      const batches = Math.ceil(count / BATCH_SIZE);

      const { assetOptions } = this.configService;
      const { assetStorageStrategy } = assetOptions;

      const { getFileName, strategy: feedStrategy } = this.options.feedStrategy;

      const feed = new feedStrategy();

      const folder = this.options.folder || "product-feed";
      const fileName = `${getFileName(channel)}.${feed.getExtention()}`;
      const filePath = `${folder}/${fileName}`;

      const passThrough = new stream.PassThrough();

      const asset = assetStorageStrategy.writeFileFromStream(
        filePath,
        passThrough
      );

      const extra = { channel, options: this.options };

      feed.create(passThrough, extra);

      for (let i = 0; i < batches; i++) {
        Logger.verbose(`Processing batch ${i + 1} of ${batches}`, loggerCtx);

        const variants = await qb
          .take(BATCH_SIZE)
          .skip(i * BATCH_SIZE)
          .getMany();

        for (let vi = 0; vi < variants.length; vi++) {
          const variant =
            await this.productPriceApplicator.applyChannelPriceAndTax(
              variants[vi],
              ctx
            );

          const translatedVariant = this.translator.translate(variant, ctx, [
            "product",
          ]);

          const stock = await this.stockLevelService.getAvailableStock(
            ctx,
            variant.id
          );

          feed.addProduct(translatedVariant, stock, extra);
        }

        observer.next({
          total: count,
          completed: Math.min((i + 1) * BATCH_SIZE, count),
        });
      }

      feed.end(extra);

      Logger.verbose(`Completed building feed ${filePath}`, loggerCtx);

      const fileLocation = await asset;

      channel.customFields.productFeedFile = fileLocation;
      channel.customFields.productFeedRebuild = false;

      await this.connection.getRepository(ctx, Channel).save(channel);

      this.eventBus.publish(
        new ProductFeedUpdatedEvent(ctx, filePath, fileName)
      );

      return {
        total: count,
        completed: count,
      };
    });
  }

  async getFeedOutputFromChannel(ctx: RequestContext) {
    const channel = await this.connection
      .getRepository(ctx, Channel)
      .findOneOrFail({ where: { id: ctx.channelId } });

    const { assetOptions } = this.configService;
    const { assetStorageStrategy } = assetOptions;

    if (!channel.customFields.productFeedFile) {
      return null;
    }

    const stream = await assetStorageStrategy.readFileToBuffer(
      channel.customFields.productFeedFile
    );
    
    return {
      stream,
      fileName: channel.customFields.productFeedFile,
    };
  }
}
