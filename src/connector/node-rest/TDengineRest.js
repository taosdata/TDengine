import {TDengineRestConnection} from './src/restConnect'

export function TDRestConnection(connection = {}) {
  return new TDengineRestConnection(connection)
}
