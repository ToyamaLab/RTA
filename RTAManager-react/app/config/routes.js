import React from 'react';
import { Router, Route, IndexRoute, hashHistory } from 'react-router';
import Main from '../components/main';
import Home from '../views/home';
import About from '../views/about';
import Ptl from '../views/ptl';
import Detail from '../views/detail';
import Content from '../views/content';
import Dashboard from '../views/dashboard';

const Routes = (
    <Router history={hashHistory} onUpdate={() => window.scrollTo(0, 0)}>
        <Route path="/" component={Main}>
            <IndexRoute component={Home} />
            <Route path="/about" component={About} />
            <Route path="/ptl" component={Ptl} />
            <Route path="/ptl/detail/:id" component={Detail} />
            <Route path="/ptl/content/:tbname" component={Content} />
            <Route path="/ptl/dashoard" component={Dashboard} />
        </Route>
    </Router>
);

export default Routes;
