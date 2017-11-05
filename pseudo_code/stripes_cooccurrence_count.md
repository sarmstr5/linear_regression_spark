class LinearRegression:
    method Map_Users(userid u, movieid m, reviews r):
        H = new AssociativeArray
        for all rating i in reviews r:
            Append movieid m to H[u]  # track all movies user u has reviewed
        for all user u in stripe_arr:
                Emit(user u, stripe H[u]) 

class Reducer:
    method Reduce_Users(userid u, stripes [H1, H2, ...]):
        Hf = new AssociativeArray
        for all stripe H in stripes [H1, H2, ...] do:
            for all movieid i in stripes H:
                for all movieid j not i in stripes H:
                        H[pair(i,j)] = H[pair(i,j)] + 1
        for all pair p in Hf:
                Emit(pair p, count Hf[p])

class Reducer:
    method Reduce_Cooccurrence_Count(pair p, counts [c1, c2, ...])
    cooccurence_sum = 0
    for all count c in counts [c1, c2, ...] do:
        cooccurence_sum = cooccurence_sum + c
    Emit(pair p, cooccurrence_sum)
