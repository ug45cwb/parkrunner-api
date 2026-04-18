import * as path from "node:path";
import * as cdk from "aws-cdk-lib/core";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { Construct } from "constructs";

export interface ApiStackProps extends cdk.StackProps {
  readonly table: dynamodb.ITable;
}

export class ApiStack extends cdk.Stack {
  public readonly api: apigateway.RestApi;

  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);
    const { table } = props;

    const leaderboardFn = new NodejsFunction(this, "LeaderboardReader", {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, "..", "lambda", "leaderboard", "index.ts"),
      handler: "handler",
      timeout: cdk.Duration.seconds(10),
      memorySize: 256,
      environment: {
        TABLE_NAME: table.tableName,
      },
      bundling: {
        minify: true,
        sourceMap: false,
        target: "node20",
      },
    });
    table.grantReadData(leaderboardFn);

    const leaderboardIntegration = new apigateway.LambdaIntegration(
      leaderboardFn,
      { proxy: true },
    );

    this.api = new apigateway.RestApi(this, "ParkrunHubRestApi", {
      restApiName: "parkrun-hub-api",
      description: "HTTP API for parkrun hub (Lambda reads DynamoDB)",
      deployOptions: {
        stageName: "prod",
        throttlingBurstLimit: 100,
        throttlingRateLimit: 50,
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
      },
    });

    const leaderboards = this.api.root.addResource("leaderboards");
    leaderboards.addMethod("GET", leaderboardIntegration);

    const byPeriod = leaderboards.addResource("{period}");
    byPeriod.addMethod("GET", leaderboardIntegration);

    this.api.root.addMethod(
      "GET",
      new apigateway.MockIntegration({
        integrationResponses: [
          {
            statusCode: "200",
            responseTemplates: {
              "application/json": JSON.stringify({
                message: "parkrun hub API",
                leaderboardsUrl: "GET /leaderboards",
              }),
            },
          },
        ],
        passthroughBehavior: apigateway.PassthroughBehavior.NEVER,
        requestTemplates: {
          "application/json": '{"statusCode": 200}',
        },
      }),
      {
        methodResponses: [{ statusCode: "200" }],
      },
    );
  }
}
