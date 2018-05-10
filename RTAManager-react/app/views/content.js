import React, { Component } from 'react'
import { Link } from 'react-router';
import axios from 'axios'
import { BootstrapTable, TableHeaderColumn } from 'react-bootstrap-table';

class Content extends Component {
  constructor(props) {
    super(props)
    this.state = {
      tbname: [],

      rows: [],
      fields: []
    }
  }

  componentDidMount() {
    axios.get('/api/ptl/content/' + this.props.params.tbname).then((response) => {
      this.setState({
        tbname: this.props.params.tbname,
        rows: response.data.rows,
        fields: response.data.fields
      })
    }).catch((response) => {
      console.log(response)
    })
  }

  render() {
    return (
      <div className="content-wrapper">
        <div className="container-fluid">
          <div className="card mb-3">
            <div className="card-header">
              <i className="fa fa-table"></i>
              { this.state.tbname }
            </div>
            <div className="card-body">
              <BootstrapTable
                data={this.state.rows}
                striped hover pagination
                version='4'
                containerStyle={{ width: '100%', height: '100%', overflowX: 'scroll', overflowY: 'scroll' }}
              >
                <TableHeaderColumn dataField="id" isKey hidden>ID</TableHeaderColumn>
              { this.state.fields.map((field, index) =>
                <TableHeaderColumn
                  dataField = { field.name }
                  dataSort
                >
                  { field.name }
                </TableHeaderColumn>
              )}
            </BootstrapTable>
            </div>
            <div className="card-footer small text-muted">
              Updated yesterday at 11:59 PM
            </div>
          </div>

        </div>
      </div>
    )
  }
}

export default Content;
