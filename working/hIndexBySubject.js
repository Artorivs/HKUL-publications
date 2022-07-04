function computeHInde (arr) {
    /** 
     * @param {Array} arr - an array of numbers
     * 
     * @return {Number} - the index of the first element in the array that is greater than or equal to the average of the array
    **/

    citations = this.citations.sort(function(a, b){return b - a});
    h_index_value = 0;

    for (var i = 0; i < citations.length; i++) {
        if (citations[i] >= i + 1) {
            h_index_value = i + 1;
        };
    };

    return h_index_value;
};

// make a class to store dictionaries of subjects and their citation counts
class SubjectCitation {
    // a constructor to store subject and citation in a dictionary
    constructor (subject, citations) {
        this.object = {};
        this.object[subject] = citations;
    };
    
    // a function to add a citation to the subject's citation count
    addCitation (subject, citations) {
        // if the subject is undefined or null, do nothing
        if (subject === undefined || subject === null) {
            return;
        } else if (!(subject in this.object)) {
            // if the subject is not in the dictionary, add it
            this.object[subject] = citations;
        } else { 
            // if the subject is in the dictionary, add the citations to the existing count
            this.object[subject] += citations;
        }
    };

    // a function to clean up the dictionary
    cleanUp () {
        for (var key in this.object) {
            if (this.object[key] === 0) {
                delete this.object[key];
            }
        }
    };

    // a function to categorise the dictionaries by an inputted map object
    categoriseByMapObject (map) {
        // create a new dictionary to store the categorised data
        this.categorised_object = {};
        // for each key in the map object, create a new dictionary to store the categorised data
        for (var key in map) {
            // if there is no key in the map object, do nothing
            if (key === undefined || key === null) {
                continue;
            } else if (key in this.object) {
                // if the key from map object is also in the original object, create a new array to store the categorised data
                    this.categorised_object[map[key]] = [];
            }
        };
        // for each key in the original object, add the data to the appropriate array
        for (var key in this.object) {
            if (key in this.categorised_object) {
                this.categorised_object[map[key]].push(this.object[key]);
            }
        }
    };
};

