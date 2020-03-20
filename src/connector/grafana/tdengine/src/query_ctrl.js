import {QueryCtrl} from 'app/plugins/sdk';
import './css/query-editor.css!'

export class GenericDatasourceQueryCtrl extends QueryCtrl {

  constructor($scope, $injector)  {
    super($scope, $injector);

    this.scope = $scope;
    this.target.target = this.target.target || 'select metric';
    this.target.type = this.target.type || 'timeserie';
  }

  onChangeInternal() {
    this.panelCtrl.refresh(); // Asks the panel to refresh data.
  }

  generateSQL(query) {
    this.lastGenerateSQL = this.datasource.generateSql( this.panelCtrl, this.target);
    this.showGenerateSQL = !this.showGenerateSQL;
  }

}

GenericDatasourceQueryCtrl.templateUrl = 'partials/query.editor.html';