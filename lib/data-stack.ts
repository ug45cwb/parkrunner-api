import * as cdk from 'aws-cdk-lib/core';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { SEED_REVISION, SEED_ROWS, type SeedRow } from './seed-data';

function toPutRequests(rows: SeedRow[]) {
  return rows.map((row) => ({
    PutRequest: {
      Item: {
        pk: { S: row.pk },
        sk: { S: row.sk },
        entityType: { S: "LEADERBOARD_ROW" },
        period: { S: row.period },
        rank: { N: String(row.rank) },
        runnerName: { S: row.runnerName },
        eventName: { S: row.eventName },
        time: { S: row.time },
      },
    },
  }));
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

export class DataStack extends cdk.Stack {
  public readonly table: dynamodb.Table;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.table = new dynamodb.Table(this, 'ParkrunHubTable', {
      tableName: 'parkrun-hub-main',
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const puts = toPutRequests(SEED_ROWS);
    const batches = chunk(puts, 25);

    batches.forEach((batch, index) => {
      const physicalId = `ddb-seed-r${SEED_REVISION}-b${index}`;
      const sdkCall: cr.AwsSdkCall = {
        service: 'DynamoDB',
        action: 'batchWriteItem',
        parameters: {
          RequestItems: {
            [this.table.tableName]: batch,
          },
        },
        physicalResourceId: cr.PhysicalResourceId.of(physicalId),
      };

      const seed = new cr.AwsCustomResource(this, `DdbSeedBatch${index}`, {
        onCreate: sdkCall,
        onUpdate: sdkCall,
        policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
          resources: [this.table.tableArn],
        }),
      });
      seed.node.addDependency(this.table);
    });
  }
}
