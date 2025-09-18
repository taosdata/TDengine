export default {
  topdesc:
    'You can follow the following steps to consume the topic `{2}` from the selected instance `{1}` of the organization `{0}`.',
  python: {
    step1: 'Install Module',
    step1desc:
      'First, you need to install the `taos-ws-py` module version >= `0.2.1`. Run the command below in your terminal.',
    step1desc1: "You'll need to have Python3 installed."
  },
  node: {
    step1: 'Install connector'
  },
  go: {
    step1: 'Initialize',
    step1desc: 'You need generate the go example model and the `driver-go` dependency:'
  },
  createProject: 'Create Project',
  step1desc: 'You can create the {0} project:',
  step1desc1: 'Then add the dependency to the `{0}` file:',
  step2: 'Configuration',
  step3: 'Create Consumer',
  step3desc: 'You can create a consumer as the following code:',
  step4: 'Subscribe Topic',
  step4desc: 'You can subscribe the shared topic `{0}` as the following code:',
  step5: 'Close Consumer',
  step5desc:
    'You can close the consume if you want to unsubscribe the messages sent by the shared topic `{0}` as the following code:',
  step6: 'Full Example',
  step6desc: 'The following are full sample codes about how to consume the shared topic `{0}`:',
  enddesc: 'For more details about data subscription, please refer to',
  enddesc1: '.',
  defaultTopic: '<TDC_TOPIC>',
  shareTopic: 'Share Topic',
  learnMoreTip:
    'To learn more about data subscription, please check <a target=\'_blank\' href="docsUrl">documentation</a>.',
  delTip: 'Are you sure to delete the "{0}" topic?',
  create: 'Create New Topic',
  delTooltip: 'Delete Topic',
  name: 'Topic Name',
  backslashTip:
    'If you want to create a topic with a case-sensitive name, please add ` before and after the name. For example, `Topic`.',
  changeDBWalRentionPeriodTip: 'Please go to ; to change the  WAL_RENTION_PERIOD of the "{0}" database.',
  fieldSet: 'Field Set',
  conditionSet: 'Condition Set',
  searchFieldTip: 'Please enter a field name to filter',
  allFieldExplanation: `You must use aggregation function for '*', such as count(*). Otherwise, the '*' column will be ignored.`,
  resultSet: 'Result Set',
  pageTitle: 'Data Subscription',
  consumerGroup: 'Consumer Group',
  clientID: 'Client ID',
  topic: 'Topic'
};
