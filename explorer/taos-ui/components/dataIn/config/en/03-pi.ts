export default {
  name: 'PI',
  id: 'pi',
  type: 'uri',
  description:
    'The Aveva PI System is a suite of software products that are used for data collection, historicizing, finding, analyzing, delivering, and visualizing. It is marketed as an enterprise infrastructure for management of real-time data and events.\n\nThe term PI System is often used to refer to the PI Server but the two are not the same. The PI System refers to all Aveva PI software products whereas the PI Server is the core product of the PI System. Data can be automatically collected from many sources (control systems, lab equipment, calculations, manual entry or custom software).\n',
  config: [
    {
      label: 'Connection Configuration',
      field: 'connection_options',
      children: [
        {
          label: 'System Configuration',
          field: 'system_configuration',
          required: true,
          defaultValue: 'PI Data Archive and Asset Framework (AF) Server',
          display_order: 0,
          type: 'select',
          options: [
            {
              label: 'PI Data Archive and Asset Framework (AF) Server',
              value: 'PI Data Archive and Asset Framework (AF) Server'
            },
            {
              label: 'PI Data Archive Only',
              value: 'PI Data Archive Only'
            }
          ]
        },
        {
          label: 'AF Server Name',
          field: 'PISystemName',
          description: 'PI System(AF Server) name (hostname).',
          required: true,
          placeholder: 'pi-af-server-name',
          display_order: 1,
          type: 'input',
          displayDependsOn: ['connection_options/system_configuration'],
          displayDependsOnValues: {
            system_configuration: ['PI Data Archive and Asset Framework (AF) Server']
          }
        },
        {
          label: 'PI Data Archive Server',
          description:
            'PI Data Archive Server (hostname).\n\nIf using an Agent, this address must be accessible from the Agent. If not using an Agent, this address must be accessible from the TDengine system.',
          field: 'host',
          required: true,
          placeholder: 'pi data archive server',
          pattern: null,
          defaultValue: '',
          display_order: 1,
          type: 'input'
        },
        {
          label: 'AF Database Name',
          description: 'AF database name',
          field: 'subject',
          required: true,
          placeholder: 'Example: Met1',
          pattern: null,
          defaultValue: '',
          type: 'input',
          displayDependsOn: ['connection_options/system_configuration'],
          displayDependsOnValues: {
            system_configuration: ['PI Data Archive and Asset Framework (AF) Server']
          }
        }
      ]
    },
    {
      label: 'Groups-before',
      field: 'groups_before',
      hide: true,
      children: []
    },
    {
      field: 'checkConnectivity',
      type: 'checkConnectivity',
      children: []
    },
    {
      label: 'Data Model Configuration',
      description:
        'Use the default configuration, or download and modify it before uploading. Configure the entry points or elements, the data model for entry, data filtering conditions, and transformation rules.',
      field: 'datasets',
      type: 'tabs',
      multiple: false,
      name: 'datasets',
      valueField: 'only-choose-one$',
      children: [
        {
          label: 'Single column mode',
          name: 'single-column',
          labelShow: false,
          labelWidth: '0px',
          category: 'single-column',
          radio: false,
          short_description:
            'The single column mode creates a super table based on the UOM of the point, with each point creating a sub table.',
          type: 'dataset',
          accept: '.csv',
          children: [
            {
              name: 'filter_value',
              display: 'Dataset filtering',
              placeholder: 'Wildcard * matches 0 or more characters, wildcard ? exactly match one character',
              options: {
                'PI Data Archive Only': [
                  {
                    label: 'point',
                    value: 'point'
                  }
                ],
                'PI Data Archive and Asset Framework (AF) Server': [
                  {
                    value: 'template',
                    label: 'template'
                  }
                ]
              },
              action: 'Download',
              action_text: 'Download default configuration',
              description:
                'Filter conditions can be specified, download default template<br>- point: filter using point names<br>- element: filter using AF element names<br>- template: filter using AF template names<br>Filter conditions can use wildcard * to match 0 or multiple characters, use wildcard? Exactly match one character',
              label: 'Dataset filtering',
              field: 'filter_value',
              defaultValue: '',
              multiple: false,
              type: 'compose',
              optionsDependsOn: 'connection_options/system_configuration'
            },
            {
              name: 'transform_config_file',
              display: 'Point configuration file',
              btnText: 'Upload configuration file',
              required: true,
              hint: {
                type: 'file'
              },
              description: 'Upload a single column mode point list file in CSV format.',
              label: 'Point configuration file',
              field: 'transform_config_file',
              defaultValue: '',
              multiple: false,
              type: 'file'
            }
          ],
          defaultValue: ''
        },
        {
          label: 'Multi column mode',
          name: 'multi-column',
          labelShow: false,
          labelWidth: '0px',
          category: 'multi-column',
          radio: true,
          short_description:
            'The multi column pattern creates a super table based on the AF Template, with each AF element creating a sub table.',
          type: 'dataset',
          accept: '.csv',
          selectable: false,
          children: [
            {
              name: 'filter_value',
              display: 'Dataset filtering',
              placeholder: 'Wildcard * matches 0 or more characters, wildcard ? exactly match one character',
              options: {
                'PI Data Archive Only': [
                  {
                    label: 'point',
                    value: 'point'
                  }
                ],
                'PI Data Archive and Asset Framework (AF) Server': [
                  {
                    value: 'template',
                    label: 'template'
                  }
                ]
              },
              action: 'Download',
              action_text: 'Download default configuration',
              description:
                'Filter conditions can be specified, download default template<br>- point: filter using point names<br>- element: filter using AF element names<br>- template: filter using AF template names<br>Filter conditions can use wildcard * to match 0 or multiple characters, use wildcard? Exactly match one character',
              label: 'Dataset filtering',
              field: 'filter_value',
              defaultValue: '',
              multiple: false,
              type: 'compose',
              optionsDependsOn: 'connection_options/system_configuration'
            },
            {
              name: 'transform_config_file',
              display: 'Model configuration file',
              required: true,
              btnText: 'Upload configuration file',
              hint: {
                type: 'file'
              },
              description: 'Upload a multi column pattern model configuration file in CSV format.',
              label: 'Model configuration file',
              field: 'transform_config_file',
              defaultValue: '',
              multiple: false,
              type: 'file',
              disabledDependsOn: ['connection_options/system_configuration'],
              disabledDependsOnValues: {
                system_configuration: ['PI Data Archive Only']
              }
            }
          ],
          defaultValue: ''
        }
      ],
      defaultValue: 'single-column'
    },
    {
      label: 'Groups-after',
      field: 'groups_after',
      hide: true,
      children: [
        {
          label: 'Auto Backfill',
          field: 'b9803c01-2434-4a8d-bd00-d8e7aa2a7732',
          description: 'Auto-backfill configurations.',
          children: [
            {
              label: 'Max Backfill Range',
              description:
                'The maximum time for automatic backfilling upon connection loss or first startup: `2d`, `3h`, `4m`, etc.\n',
              field: 'MaxBackfillRangeDays',
              placeholder: 'The value is an integer ranging [0,600]',
              defaultValue: '0m',
              pattern: null,
              patternMsg: 'The value can only be a positive integer or 0',
              grid_two: false,
              type: 'composeAppend',
              options: [
                {
                  value: 'd',
                  label: 'Day'
                },
                {
                  value: 'h',
                  label: 'Hours'
                },
                {
                  value: 'm',
                  label: 'Mniute'
                },
                {
                  value: 's',
                  label: 'Second'
                }
              ],
              min: 0,
              max: 600
            }
          ],
          hide: false
        }
      ]
    },
    {
      label: 'Advanced Options',
      field: 'advanced_options',
      description:
        'Advanced options including read/write concurrency, collection options, performance tuning, etc. Users can leave\nthese options as default to use the recommended settings.\n',
      type: 'collapse',
      defaultValue: true,
      collapsible: 'one',
      children: [
        {
          label: 'Synchronize New Elements',
          field: 'sync_add_element',
          description:
            'Monitor the newly added elements under the configured templates, and synchronize the data of the newly added elements without restarting the task',
          defaultValue: true,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Synchronize The Changes of Static Attribute',
          field: 'sync_update_attribute',
          description: 'Synchronize the changes of all static attribute to TDengine',
          defaultValue: true,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Synchronize The Deletions of Elements',
          field: 'sync_delete_element',
          description:
            'Monitor deleting elements under the configured templates, and correspondingly drop the corresponding child tables in TDengine',
          defaultValue: true,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Synchronize The Deletion of Point Data',
          field: 'sync_delete_data',
          description:
            'For the dynamic attributes of an element, if the data for a certain period of time is deleted in PI, the corresponding data is set to null in TDengine',
          defaultValue: true,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Synchronize The Changes of Point Data',
          field: 'sync_update_data',
          description:
            'For the dynamic attributes of an element, if the data for a certain time is modified in PI, the corresponding data is updated automatically too in TDengine',
          defaultValue: true,
          required: false,
          hint: {
            type: 'bool'
          },
          type: 'switch'
        },
        {
          label: 'Log Level',
          field: 'log_level',
          description:
            'Adjust the log level of the data source as required. This parameter does not always take effect.',
          defaultValue: 'info',
          required: false,
          hint: {
            type: 'str',
            choices: ['error', 'warn', 'info', 'debug', 'trace']
          },
          type: 'select',
          options: [
            {
              label: 'error',
              value: 'error'
            },
            {
              label: 'warn',
              value: 'warn'
            },
            {
              label: 'info',
              value: 'info'
            },
            {
              label: 'debug',
              value: 'debug'
            },
            {
              label: 'trace',
              value: 'trace'
            }
          ]
        },
        {
          label: 'Batch Size',
          field: 'batch_size',
          description:
            'The number of data points to be written in a single request. The default value is 1000. If the data source is slow to respond, you can reduce this value appropriately.\n',
          defaultValue: '1000',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 10000
          },
          type: 'number',
          min: 1,
          max: 10000
        },
        {
          label: 'Batch Timeout',
          field: 'batch_timeout',
          description:
            'The maximum time(in seconds) to wait before sending a batch of data points. The default value is 1s. If the data source is slow to respond, you can increase this value appropriately.\n',
          defaultValue: '1',
          required: false,
          hint: {
            type: 'integer',
            min: 1,
            max: 60
          },
          type: 'number',
          min: 1,
          max: 60
        },
        {
          label: 'Health Check Duration',
          field: 'health_check_window_in_second',
          description:
            'Indicates the time duration for monitoring the task status. Typically in minutes, this duration applies uniformly to all health states.',
          defaultValue: '0s',
          placeholder: 'Enter an integer in the range [0, 60000]',
          required: false,
          hint: {
            type: 'duration',
            choices: [
              {
                value: 's',
                label: 'Seconds'
              }
            ],
            min: 0,
            max: 60000
          },
          type: 'composeAppend',
          options: [
            {
              value: 's',
              label: 'Seconds'
            }
          ],
          min: 0,
          max: 60000
        },
        {
          label: 'Busy State Threshold',
          field: 'busy_threshold',
          description:
            'Percentage indicating the ratio of the number of elements enqueued to the total queue length. Default is 100%.',
          defaultValue: '100%',
          required: false,
          hint: {
            type: 'duration',
            choices: [
              {
                label: '%',
                value: '%'
              }
            ],
            min: 0,
            max: 100
          },
          type: 'composeAppend',
          options: [
            {
              label: '%',
              value: '%'
            }
          ],
          min: 0,
          max: 100
        },
        {
          label: 'Max Write Queue Length',
          field: 'max_queue_length',
          description: 'Indicates the maximum write queue length for a single IPC connection.',
          defaultValue: '1000',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 10000
          },
          type: 'number',
          min: 0,
          max: 10000
        },
        {
          label: 'Write Error Threshold',
          field: 'max_errors_in_window',
          description:
            'Indicates the number of allowed write errors during the health check duration. Exceeding the threshold will trigger a Fatal alert.',
          defaultValue: '10',
          required: false,
          hint: {
            type: 'integer',
            min: 0,
            max: 10000
          },
          type: 'number',
          min: 0,
          max: 10000
        }
      ]
    }
  ]
};
