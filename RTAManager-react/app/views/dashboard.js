import React, { Component } from 'react'
import { Link } from 'react-router';
import axios from 'axios'

class Dashboard extends Component {
  constructor(props) {
    super(props)
    this.state = {
      rows: []
    }
  }

  componentDidMount() {
    axios.get('/api/ptl').then((response) => {
      this.setState({
        rows: response.data.rows,
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
               Public Table Library
            </div>
            <div className="card-body">
              <div className="table-responsive">
                <table className="table table-striped" width="100%" id="dataTable" >
                  <thead>
                    <tr>
                      <th>Access Name</th>
                      <th>Description</th>
                      <th>Detail</th>
                      <th>Content</th>
                      <th>Download</th>
                    </tr>
                  </thead>
                  <tfoot>
                    <tr>
                      <th>Access Name</th>
                      <th>Description</th>
                      <th>Detail</th>
                      <th>Content</th>
                      <th>Download</th>
                    </tr>
                  </tfoot>
                  <tbody>
                    { this.state.rows.map((row) =>
                      <tr>
                        <td>{row.access_name}</td>
                        <td>{row.description}</td>
                        <td><Link to={"/ptl/detail/" + row.id}>Detail</Link></td>
                        <td><Link to={"/ptl/content/" + row.access_name}>Content</Link></td>
                        <td>Download</td>
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

export default Dashboard;
