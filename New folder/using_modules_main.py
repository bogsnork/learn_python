#importing a module
import mymodule

#accessing a function
mymodule.greeting("Jonathan")

#accessing a library
a = mymodule.person1["age"]
print(a)

#importing and renaming a module
import mymodule as mx

#accessing a function
mx.greeting("Jonathan")
#accessing a library
a = mx.person1["age"]
print(a)

#import a built in module
import platform

#use the dir() function
x = dir(mymodule) #reads module functions and other stuff
print(x)

#import parts of a  module
from mymodule import person1
print(person1["age"])

#import more parts
from mymodule import person1, greeting