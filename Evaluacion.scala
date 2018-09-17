////////////////////1
def Pares(par:Int):Boolean ={return (par%2==0)}
var par = 4
println(Pares(par))

def Pars(num:Int): Boolean = {
  if(num%2==0){
    return true
  }
return false
}
var num = 8
println(Pars(num))

////////////////////2
val numbers= List (1,4,5,7)
def check(num:List[Int]): Boolean ={ return (num(1)%2==0)}
println(check(numbers))

//3
def numero(num:Int):Int{
   if(num==7){return 14}
   return num
}
val lista = List(1,2,7)
var sum = numero(lista(0)) + numero(lista(1)) + numero(lista(2))


////////////5
def palin(palabra:String):Boolean ={
  return (palabra==palabra.reverse)
}
 val palabra = "oso"
 val palabra2 = "casa"

 println(palin(palabra))
 println(palin(palabra2))
