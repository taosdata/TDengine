import {DataSourcePlugin} from '@grafana/data'
import {DataSource} from './datasource';
import {ConfigEditor, QueryEditor} from './components'
import {DataSourceOptions, Query} from './types';

export const plugin = new DataSourcePlugin<DataSource, Query, DataSourceOptions>(DataSource)
    .setConfigEditor(ConfigEditor)
    .setQueryEditor(QueryEditor)
