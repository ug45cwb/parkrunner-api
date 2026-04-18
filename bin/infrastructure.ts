#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { ApiStack } from '../lib/api-stack';
import { ComputeStack } from '../lib/compute-stack';
import { DataStack } from '../lib/data-stack';

const app = new cdk.App();

const env =
  process.env.CDK_DEFAULT_ACCOUNT && process.env.CDK_DEFAULT_REGION
    ? {
        account: process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEFAULT_REGION,
      }
    : undefined;

const dataStack = new DataStack(app, 'ParkrunHubDataStack', {
  description: 'Data layer: DynamoDB and related storage',
  env,
});

new ComputeStack(app, 'ParkrunHubComputeStack', {
  description: 'Compute layer: Step Functions workflows',
  env,
});

const apiStack = new ApiStack(app, 'ParkrunHubApiStack', {
  description: 'API layer: Amazon API Gateway',
  env,
  table: dataStack.table,
});
apiStack.addDependency(dataStack);
