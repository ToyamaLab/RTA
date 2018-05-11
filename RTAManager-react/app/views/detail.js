import React, { Component } from 'react'
import { Link } from 'react-router';
import axios from 'axios'

class Detail extends Component {
  constructor(props) {
    super(props)
    this.state = {
      tbname: [],
      rows: [],
      fields: []
    }
  }

  componentDidMount() {
    axios.get('/api/ptl/detail/' + this.props.params.id).then((response) => {
      this.setState({
        tbname: response.data.rows[0].table_name,
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
              <div className="table-responsive">
                <table className="table table-striped" width="100%" id="dataTable" >
                  <thead>
                    <tr>
                      <th>Column Name</th>
                      <th>Data Type</th>
                      <th>Description</th>
                    </tr>
                  </thead>
                  <tfoot>
                    <tr>
                      <th>Column Name</th>
                      <th>Data Type</th>
                      <th>Description</th>
                    </tr>
                  </tfoot>
                  <tbody>
                    { this.state.rows.map((row) =>
                      <tr>
                          <td>{row.name}</td>
                          <td>{row.type}</td>
                          <td>{row.description}</td>
                      </tr>
                      )
                    }
                  </tbody>
                </table>
              </div>
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

export default Detail;
