const mongodb= require('mongodb')
const async = require('async')
const url = 'mongodb://localhost:27017'
const customers = require('./m3-customer-data')
const customerAddresses = require('./m3-customer-address-data')

let tasks = []
const limit = parseInt(process.argv[2], 10) || 1000
mongodb.MongoClient.connect(url, (error, cilent) => {
    if (error) return process.exit(1)
    let db = cilent.db('edx-course-db')

    customers.forEach((customer, index) => {
        // customers is actually not immuateable unless using Object.freeze()
        customers[index] = Object.assign(customer, customerAddresses[index])
    
        if (index % limit == 0) {
            let start = index
            let end = (start + limit > customers.length) ? customers.length : start + limit
            tasks.push((done) => {
                console.log(`Processing ${start} - ${end - 1} out of ${customers.length - 1}`)
                // Not include "end"
                db.collection('customers').insert(customers.slice(start, end), (error, results) => {
                    done(error, results)
                })
            })
        } 
    })
    console.log(`Launching ${tasks.length} parallel ${tasks.length == 1 ? 'task' : 'tasks'}`)
    let startTime = Date.now()
    async.parallel(tasks, (error, results) => {
        if (error) console.error(error)
        let endTime = Date.now()
        console.log(`Execution time: ${endTime-startTime} ms`)
        // console.log(results)
        cilent.close()
    })
})