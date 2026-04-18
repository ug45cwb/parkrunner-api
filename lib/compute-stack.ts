import * as cdk from 'aws-cdk-lib/core';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import { Construct } from 'constructs';

export class ComputeStack extends cdk.Stack {
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const start = new sfn.Pass(this, 'Start', {
      comment: 'Placeholder step; extend with Lambdas or parallel branches as needed.',
      result: sfn.Result.fromObject({ status: 'ok' }),
    });
    const done = new sfn.Succeed(this, 'Done');

    const definition = start.next(done);

    this.stateMachine = new sfn.StateMachine(this, 'ParkrunHubWorkflow', {
      stateMachineName: 'parkrun-hub-workflow',
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      timeout: cdk.Duration.minutes(5),
    });
  }
}
