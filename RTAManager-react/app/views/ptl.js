import React, { Component } from 'react'
import { Link } from 'react-router';
import axios from 'axios'
import { BootstrapTable, TableHeaderColumn } from 'react-bootstrap-table';

class Ptl extends Component {
  constructor(props) {
    super(props)
    this.state = {
      rows: []
    }
  }

  componentDidMount() {
    axios.get('/api/ptl').then((response) => {
      this.setState({
        rows: response.data.rows
      })
    }).catch((response) => {
      console.log(response)
    })
  }

  renderLink(cell, row, page) {
    return (
      <Link to={ `/ptl/${page}/${row.id}` }>{ page }</Link>
    )
  }


    renderContentLink(cell, row) {
        return (
            <Link to={ `/ptl/content/${row.access_name}` }>Content</Link>
        )
    }

  render() {
    return (
      <div className="content-wrapper">
        <div className="container-fluid">
          <div className="card mb-3">
            <div className="card-header">
              <i className="fa fa-table"></i>
                Publiic Table Library
            </div>
            <div className="card-body">
              <BootstrapTable
                  data={this.state.rows}
                  striped hover pagination
                  version='4'
                  containerStyle={{ width: '100%', height: '100%', overflowX: 'scroll', overflowY: 'scroll' }}
              >
                <TableHeaderColumn dataField="id" isKey hidden>ID</TableHeaderColumn>
                <TableHeaderColumn dataField="access_name">Access Name</TableHeaderColumn>
                <TableHeaderColumn dataField="description">Description</TableHeaderColumn>
                <TableHeaderColumn dataFormat={ this.renderLink } formatExtraData="detail">Detail</TableHeaderColumn>
                <TableHeaderColumn dataFormat={ this.renderContentLink }>Content</TableHeaderColumn>
                <TableHeaderColumn dataFormat={ this.renderLink } formatExtraData="download">Download</TableHeaderColumn>
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

export default Ptl;
