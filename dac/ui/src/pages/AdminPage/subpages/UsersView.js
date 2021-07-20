/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import './Users.less'; // TODO to Vasyl, need to use Radium
import { PureComponent } from 'react';
import Immutable from 'immutable';
import PropTypes from 'prop-types';
import { injectIntl, FormattedMessage } from 'react-intl';
import { Link } from 'react-router';
import Radium from 'radium';
import { createSelector } from 'reselect';

import SettingHeader from '@app/components/SettingHeader';
import Art from '@app/components/Art';
import FontIcon from 'components/Icon/FontIcon';

import StatefulTableViewer from 'components/StatefulTableViewer';

import { pageContent, page } from 'uiTheme/radium/general';
import { DeleteButton } from '@app/components/tableRowButtons/DeleteButton';

const getPathname = (location) => location.pathname;
const getHash = (location) => location.hash;
const getSearch = (location) => location.search;

const userLinkToSelector = createSelector(
  [getPathname, getHash, getSearch],
  (pathname, hash, search) => ({
    pathname,
    hash,
    search,
    state: {modal: 'EditUserModal'}
  }));

@Radium
class UsersView extends PureComponent {
  static propTypes = {
    users: PropTypes.instanceOf(Immutable.List),
    removeUser: PropTypes.func.isRequired,
    viewState: PropTypes.instanceOf(Immutable.Map),
    search: PropTypes.func,
    intl: PropTypes.object
  }

  static contextTypes = {
    location: PropTypes.object.isRequired,
    loggedInUser: PropTypes.object.isRequired
  };

  componentDidMount() { // todo: is anything using this?
    this.refs.searchInputs.value = this.context.location.query.filter || '';
  }

  getTableColumns() {
    // set width for fixed width columns. If width is not provided, then flexGrow will be applied
    return [
      {
        label: la('Name'),
        flexGrow: 1
      },
      {
        label: la('Username'),
        flexGrow: 1
      },
      {
        label: la('Email'),
        width: 250
      },
      {
        label: la('Action'),
        width: 120
      }
    ].map((colConfig, i) => ({
      key: i + '',
      ...colConfig
    }));
  }

  getTableData() { // todo: styling: col alignment and spacing
    return this.props.users.map((user, index) => {
      const userName = user.getIn(['userConfig', 'userName']);
      const editUserLink = {
        pathname: '/admin/users',
        state: {
          modal: 'EditUserModal',
          userId: user.get('id')
        }
      };

      const fullName = [
        user.getIn(['userConfig', 'firstName']),
        user.getIn(['userConfig', 'lastName'])
      ].filter(Boolean).join(' ');

      return {
        rowClassName: userName,
        data: [
          // todo: with good i18n this should be givenName and familyName, and the order should be dependent on the locale of that user
          <span style={styles.nameHolder}>
            <span>{fullName}</span>
          </span>,
          user.getIn(['userConfig', 'userName']),
          user.getIn(['userConfig', 'email']),
          <span className='actions-wrap' style={styles.actionBtnWrap} key={index}>
            <Link to={editUserLink} data-qa='edit-user'>
              <button style={styles.actionBtn}><FontIcon type='Edit' theme={styles.actionIcon}/></button>
            </Link>
            {
              this.context.loggedInUser.userName !== userName && <DeleteButton
                onClick={this.props.removeUser.bind(this, user)}
                dataQa='delete-user'
              />
            }
          </span>
        ]
      };
    });
  }

  renderAddUsersButton = () => {
    const addUserLinkTo = userLinkToSelector(this.context.location);

    return (
      <Link to={addUserLinkTo} data-qa='add-user-link' className='settingHeader__action'>
        <Art src='PlusSolid.svg' alt='+' className='settingPage__icon margin-right'/>
        <FormattedMessage id='Admin.UserManagement.Users.Add' />
      </Link>
    );
  }

  renderHeader() {
    const {
      intl: {
        formatMessage
      } = {}
    } = this.props;
    return (
      <SettingHeader
        title={formatMessage({ id: 'Admin.UserManagement.Users' })}
        endChildren={this.renderAddUsersButton()}
      />
    );
  }

  render() {
    const { viewState } = this.props;
    const columns = this.getTableColumns();
    const tableData = this.getTableData();
    return (
      <div id='admin-user' style={page}>
        {this.renderHeader()}
        {
          <div className='filter user'>
            <div className='search-wrap' style={styles.searchWrap}>
              <FontIcon
                type='Search'
                theme={styles.fontIcon}/>
              <input
                type='text'
                placeholder={la('Search users')}
                style={styles.searchInput}
                onChange={this.props.search}
                ref='searchInputs'
              />
            </div>
          </div>
        }
        <div style={[pageContent, {position: 'relative'}]}>
          <StatefulTableViewer
            columns={columns}
            tableData={tableData}
            viewState={viewState}
          />
        </div>
      </div>
    );
  }
}

const styles = {
  nameHolder: {
    display: 'flex',
    alignItems: 'center',
    paddingLeft: '8px'
  },
  // pending server support:
  //   userAvatar: {
  //     width: '22px',
  //     height: '22px',
  //     margin: '0 10px 0 5px'
  //   },
  searchWrap: {
    clear: 'both',
    margin: '10px 0',
    position: 'relative',
    width: 300
  },
  searchInput: {
    display: 'block',
    fontSize: 12,
    border: '1px solid rgba(0,0,0, .1)',
    borderRadius: 2,
    width: '100%',
    padding: '5px 10px',
    outline: '0'
  },
  fontIcon: {
    'Icon': {
      'width': 22,
      'height': 22
    },
    'Container': {
      'position': 'absolute',
      'right': '3px',
      'top': '0',
      'bottom': '0',
      'margin': 'auto',
      'width': 22,
      'height': 22
    }
  },
  actionBtnWrap: {
    display: 'flex',
    alignItems: 'center'
  },
  actionBtn: { // todo: DRY this up with other action buttons in the project
    border: 0,
    boxShadow: '0 1px 1px #c2c2c2',
    borderRadius: 2,
    background: '#eee',
    height: 23,
    width: 42,
    overflow: 'hidden',
    display: 'flex',
    alignItems: 'center',
    ':hover':{
      background: '#fff'
    }
  },
  actionIcon: {
    'Icon': {
      width: 22,
      height: 22
    },
    'Container': {
      width: 22,
      height: 22,
      margin: '0 auto'
    }
  }
};

export default injectIntl(UsersView);
