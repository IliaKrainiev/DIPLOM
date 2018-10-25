Promise.all([
    new Promise((res) => {
        res(console.log('aaaaa'));
    })]).then(() => {
    console.log('bbb')
});