import {
  Controller,
  Get,
  Res,
  HttpStatus,
  Query,
} from "@nestjs/common";
import { Response } from "express";
import {
  Ctx,
  Logger,
  RequestContext,
  RequestContextService,
} from "@vendure/core";
import { ProductFeedBuilderService } from "../service/product-feed-builder.service";
import mime from "mime";

@Controller("feed")
export class ProductController {
  constructor(
    private productFeedBuilderService: ProductFeedBuilderService,
    private requestContextService: RequestContextService
  ) {}

  @Get()
  async findAll(
    @Ctx() ctx: RequestContext,
    @Res() reponse: Response,
    @Query("token") token?: string
  ) {
    const context = await (token
      ? this.requestContextService.create({
          apiType: "shop",
          channelOrToken: token,
        })
      : Promise.resolve(ctx));

    Logger.info(
      `Requesting product feed for channel "${context.channel.code}"`
    );

    if (context.channel.customFields.productFeedOutput !== "url") {
      Logger.info(
        `Channel "${context.channel.code}" is not configured for product  feed`
      );
      return reponse.status(HttpStatus.NOT_FOUND);
    }

    const output =
      await this.productFeedBuilderService.getFeedOutputFromChannel(
        context
      );

    if (!output) return reponse.status(HttpStatus.NOT_FOUND);

    const { fileName, stream } = output;

    const mimeType = mime.lookup(fileName);
    reponse.setHeader("Content-Type", mimeType);

    reponse.send(stream.toString());
  }
}
