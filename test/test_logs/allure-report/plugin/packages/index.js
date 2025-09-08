'use strict';

allure.api.addTranslation('en', {
    tab: {
        packages: {
            name: 'Packages'
        }
    }
});

allure.api.addTranslation('ru', {
    tab: {
        packages: {
            name: 'Пакеты'
        }
    }
});

allure.api.addTranslation('zh', {
    tab: {
        packages: {
            name: '包'
        }
    }
});

allure.api.addTranslation('de', {
    tab: {
        packages: {
            name: 'Pakete'
        }
    }
});

allure.api.addTranslation('nl', {
    tab: {
        packages: {
            name: 'Packages'
        }
    }
});

allure.api.addTranslation('he', {
    tab: {
        packages: {
            name: 'חבילות'
        }
    }
});

allure.api.addTranslation('br', {
    tab: {
        packages: {
            name: 'Pacotes'
        }
    }
});

allure.api.addTranslation('ja', {
    tab: {
        packages: {
            name: 'パッケージ'
        }
    }
});

allure.api.addTranslation('es', {
    tab: {
        packages: {
            name: 'Paquetes'
        }
    }
});

allure.api.addTranslation('kr', {
    tab: {
        packages: {
            name: '패키지'
        }
    }
});

allure.api.addTranslation('fr', {
    tab: {
        packages: {
            name: 'Paquets'
        }
    }
});

allure.api.addTranslation('pl', {
    tab: {
        packages: {
            name: 'Pakiety'
        }
    }
});

allure.api.addTranslation('am', {
    tab: {
        packages: {
            name: 'Փաթեթներ'
        }
    }
});

allure.api.addTranslation('az', {
    tab: {
        packages: {
            name: 'Paketlər'
        }
    }
});

allure.api.addTranslation('sv', {
    tab: {
        packages: {
            name: 'Paket'
        }
    }
});

allure.api.addTranslation('isv', {
    tab: {
        packages: {
            name: 'Pakety'
        }
    }
});

allure.api.addTranslation('ka', {
    tab: {
        packages: {
            name: 'პაკეტები'
        }
    }
});

allure.api.addTranslation('it', {
    tab: {
        packages: {
            name: 'Pacchetti'
        }
    }
});

allure.api.addTab('packages', {
    title: 'tab.packages.name', icon: 'fa fa-align-left',
    route: 'packages(/)(:testGroup)(/)(:testResult)(/)(:testResultTab)(/)',
    onEnter: (function (testGroup, testResult, testResultTab) {
        return new allure.components.TreeLayout({
            testGroup: testGroup,
            testResult: testResult,
            testResultTab: testResultTab,
            tabName: 'tab.packages.name',
            baseUrl: 'packages',
            url: 'data/packages.json'
        });
    })
});
