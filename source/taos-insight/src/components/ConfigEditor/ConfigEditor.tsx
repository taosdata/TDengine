import React, {ReactElement,useEffect} from 'react'
import {Button, Modal, ConfirmModal, FieldSet, LegacyForms, Switch, Tab, TabContent, TabsBar, Icon} from '@grafana/ui'
import type {EditorProps} from './types'
import {useChangeSecureOptions} from './useChangeSecureOptions'
import {useResetSecureOptions} from './useResetSecureOptions'
import {deleteAlerts, checkGrafanaVersion, getCurrentTime} from '../../utils'

import './ConfigEditor.css'

const {SecretFormField, FormField} = LegacyForms;

enum authType {Basic = 'basic', Token = 'token'}

export function ConfigEditor(props: EditorProps): ReactElement {
    const {secureJsonData} = props.options;
    const {secureJsonFields} = props.options;
    const onChangeUrl = useChangeSecureOptions(props, 'url')
    const onResetUrl = useResetSecureOptions(props, 'url')
    const onChangeUser = useChangeSecureOptions(props, 'user')
    const onResetUser = useResetSecureOptions(props, 'user')
    const onChangePassword = useChangeSecureOptions(props, 'password')
    const onResetPassword = useResetSecureOptions(props, 'password')
    const onChangeToken = useChangeSecureOptions(props, 'token')
    const onResetToken = useResetSecureOptions(props, 'token')

    // console.log("ConfigEditor:" + secureJsonData?.token)
    // console.log("secureJsonFields.token:" + secureJsonFields.token)
    const [active, setActive] = React.useState((secureJsonFields && secureJsonFields.token) ? authType.Token : authType.Basic )
    let alertState = true
    if (props.options.jsonData.isLoadAlerts === undefined) {
        props.options.jsonData.isLoadAlerts = true;
        alertState = props.options.jsonData.isLoadAlerts
    } else {
        alertState = props.options.jsonData.isLoadAlerts
    }

    if (props.options.jsonData.folderUidSuffix === undefined) {
        props.options.jsonData.folderUidSuffix= getCurrentTime();
    }

    const [isVisible, setisVisible] = React.useState(false);
    useEffect(() => {
        const performVersionCheck = async () => {
            const supported = await checkGrafanaVersion();
            setisVisible(supported);
        };
        performVersionCheck();
    }, []);

    const [openConfirm, setOpenConfirm] = React.useState(false);

    const [openAlert, setOpenAlert] = React.useState(false);

    const [openAlertMessage, setOpenAlertMessage] = React.useState("");
    /**
     * Here is the cleaning of alarm rules.
     * When executing the onConfirm method, the testDatasource() function will be called,making it difficult to distinguish whether to delete or add.
     * Therefore, during the onDismiss event, a deletion operation will be performed, but the button names will be swapped
     */
    const clearAlertRules = () => {
        updateLoadStatus(false);
        setOpenConfirm(false);
        deleteAlerts(props.options.uid).then(()=>{
            return;
        }).catch((e: any) => {
            setOpenAlert(true);
            setOpenAlertMessage("Failed to delete alarm rules, reason: " + e.message)
            throw e;
        });
        
    };

    const [alertRule, setAlert] = React.useState(alertState);
    const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
        updateLoadStatus(event.target.checked);
    };

    const updateLoadStatus = (checked: boolean) => {
        setAlert(checked);
        const {onOptionsChange, options} = props;
        props.options.jsonData.isLoadAlerts = checked
        onOptionsChange({
            ...options,
            jsonData: {
                ...options.jsonData,
            },
        })
    };

    return (
        <div className='gf-form-group'>
            <FieldSet label="TDengine Host">
                <div className='gf-form max-width-30'>
                    <FormField label='Host'
                               labelWidth={8}
                               inputWidth={22}
                               tooltip="datasource's host"
                               onChange={onChangeUrl}
                               onBlur={onChangeUrl}
                               onReset={onResetUrl}
                               value={props.options.url || ''}
                               placeholder='http://localhost:6041'
                               required={true}
                               disabled={props.options.readOnly}
                    />
                </div>
            </FieldSet>
            <FieldSet label="TDengine Authentication">
                <TabsBar>
                    <Tab label={'by user and password'}
                         active={active === authType.Basic}
                         onChangeTab={() => setActive(authType.Basic)}
                    />
                    <Tab label={'by token'}
                         active={active === authType.Token}
                         onChangeTab={() => setActive(authType.Token)}
                    />
                </TabsBar>
                <TabContent>
                    {
                        active === authType.Basic &&
                        <div>
                            <div className='gf-form max-width-20'>
                                <FormField label='User'
                                           labelWidth={8}
                                           inputWidth={10}
                                           tooltip="datasource's username"
                                           onChange={onChangeUser}
                                           onBlur={onChangeUser}
                                           onReset={onResetUser}
                                           value={props.options.user || ''}
                                           disabled={props.options.readOnly}
                                           placeholder={secureJsonFields.basicAuth ? 'basic_auth configured' : 'username'}
                                />
                            </div>
                            <div className='gf-form max-width-20'>
                                <SecretFormField
                                    isConfigured={(secureJsonFields && secureJsonFields.password) as boolean}
                                    value={secureJsonData?.password || ''}
                                    label='Password'
                                    tooltip="datasource's password"
                                    labelWidth={8}
                                    inputWidth={10}
                                    onReset={onResetPassword}
                                    onChange={onChangePassword}
                                    onBlur={onChangePassword}
                                    disabled={props.options.readOnly || (secureJsonFields && secureJsonFields.password) as boolean}
                                    placeholder={secureJsonFields.basicAuth ? 'basic_auth configured' : 'password'}
                                />
                            </div>
                        </div>
                    }
                    {
                        active === authType.Token &&
                        <div className='gf-form max-width-30'>
                            <SecretFormField
                                isConfigured={(secureJsonFields && secureJsonFields.token) as boolean}
                                value={secureJsonData?.token || ''}
                                label='Token'
                                tooltip="datasource's cloud token"
                                placeholder='token of TDengine cloud'
                                labelWidth={8}
                                inputWidth={22}
                                onReset={onResetToken}
                                onChange={onChangeToken}
                                onBlur={onChangeToken}
                                disabled={props.options.readOnly || (secureJsonFields && secureJsonFields.token) as boolean}
                            />
                        </div>
                    }
                </TabContent>
            </FieldSet>
            <div>
                {isVisible && (
                    <FieldSet label="TDengine Alert">
                        <div className='gf-form max-width-20'>
                            <FormField label="Load TDengine Alert"
                                       labelWidth={10}
                                       className='align-center'
                                       inputEl= {
                                           <Switch
                                               onChange={handleSwitchChange}
                                               value={alertRule}
                                           />
                                       }
                            />
                        </div>
                        <div className='gf-form max-width-20'>
                            <FormField label="Clear TDengine Alert"
                                       labelWidth={10}
                                       className='align-center'
                                       inputEl= {
                                           <Button variant="destructive"
                                                   onClick={() => setOpenConfirm(true)}
                                                   title="Clear the TDengine alerts"
                                                   icon="trash-alt"
                                                   size="sm"
                                           ></Button>
                                       }
                            />
                        </div>
                        <ConfirmModal
                            isOpen={openConfirm}
                            title="Clear TDengine Alerts"
                            body="Are you sure you want to clear alerts?"
                            confirmText="Cancel"
                            dismissText="Confirm"
                            icon="exclamation-triangle"
                            onConfirm={() => setOpenConfirm(false)}
                            onDismiss={() => clearAlertRules()}
                        />

                        <Modal isOpen={openAlert} title={<div><Icon name='x' color='red' size='xxl' /> <label style={{ color: 'red', fontSize: '16px' }}>Failed to delete rules</label></div>} onDismiss={() => setOpenAlert(false)}>
                            <p style={{ fontSize: '16px' }}>{openAlertMessage}</p>
                        </Modal>


                    </FieldSet>
                )}
            </div>
        </div>
    )
}

